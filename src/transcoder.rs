use std::ffi::{CStr, CString, c_int};
use std::io::Write;
use std::os::raw::c_void;
use std::path::Path;
use std::ptr;
use std::slice;

use anyhow::Context;
use ffmpeg_sys_next::{
    AV_CH_LAYOUT_MONO, AVChannelLayout, AVChannelOrder, AVCodec, AVCodecContext, AVCodecParameters,
    AVERROR, AVERROR_EOF, AVFormatContext, AVFrame, AVMediaType, AVPacket, AVSampleFormat,
    AVStream, SwrContext, av_dict_get, av_find_best_stream, av_frame_alloc, av_frame_free,
    av_frame_unref, av_freep, av_opt_set_int, av_packet_alloc, av_packet_free, av_packet_unref,
    av_read_frame, av_samples_alloc_array_and_samples, avcodec_alloc_context3,
    avcodec_find_decoder, avcodec_free_context, avcodec_open2, avcodec_parameters_to_context,
    avcodec_receive_frame, avcodec_send_packet, avformat_close_input, avformat_find_stream_info,
    avformat_open_input, swr_alloc, swr_alloc_set_opts2,
};

// EAGAIN as ffmpeg_sys_next re-exports it isn't a plain c_int, so pull it from libc-style errno.
const EAGAIN: c_int = 11;

// --------------------------------------------------------------------------
// RAII guards
//
// Each FFmpeg resource gets its own tiny wrapper with a Drop impl that frees
// it exactly once, on every exit path (success, `?`, panic-unwind). This is
// what replaces the ~5 duplicated manual cleanup blocks in the original code.
// --------------------------------------------------------------------------

struct FormatContextGuard(*mut AVFormatContext);

impl Drop for FormatContextGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { avformat_close_input(&mut self.0) };
        }
    }
}

struct CodecContextGuard(*mut AVCodecContext);

impl Drop for CodecContextGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { avcodec_free_context(&mut self.0) };
        }
    }
}

struct SwrContextGuard(*mut SwrContext);

impl Drop for SwrContextGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { ffmpeg_sys_next::swr_free(&mut self.0) };
        }
    }
}

struct PacketGuard(*mut AVPacket);

impl Drop for PacketGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { av_packet_free(&mut self.0) };
        }
    }
}

struct FrameGuard(*mut AVFrame);

impl Drop for FrameGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { av_frame_free(&mut self.0) };
        }
    }
}

/// A heap buffer allocated by `av_samples_alloc_array_and_samples`. Frees the
/// sample buffer *and* the pointer array it lives in, exactly once.
struct SampleBufferGuard(*mut *mut u8);

impl Drop for SampleBufferGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                av_freep(self.0 as *mut c_void);
                av_freep(&mut self.0 as *mut *mut *mut u8 as *mut c_void);
            }
        }
    }
}

// Transcoder metadata

pub struct TranscoderMetadata {
    pub input_sample_rate: usize,
    pub resampled_file_size: usize,
}

/// Extracts and transcodes the best (or first English-tagged) audio stream of
/// `input_file_path` into a mono, 16 kHz, f32 PCM file at `output_file_path`.

pub fn transcoder(
    input_file_path: String,
    tmpfile: &mut tempfile::NamedTempFile,
) -> anyhow::Result<TranscoderMetadata> {
    let target_path = Path::new(&input_file_path);

    if !target_path.exists() {
        return Err(anyhow::anyhow!("Target file path doesn't exist!"));
    }

    unsafe {
        // casting rust string to cstring
        let c_target_path =
            CString::new(input_file_path).context("Input path contains a NUL byte")?;

        // ---- open input & probe streams -----------------------------------
        let mut format_context_ptr: *mut AVFormatContext = ptr::null_mut();
        let result: c_int = avformat_open_input(
            &mut format_context_ptr,
            c_target_path.as_ptr(),
            ptr::null_mut(),
            ptr::null_mut(),
        );
        if result < 0 {
            return Err(anyhow::anyhow!(
                "Failed to initialize avformat & open input file!"
            ));
        }
        let format_guard = FormatContextGuard(format_context_ptr);

        // stream info
        if avformat_find_stream_info(format_context_ptr, ptr::null_mut()) < 0 {
            return Err(anyhow::anyhow!("Failed to find stream info!"));
        }

        // total stream count and selected stream
        let mut selected_stream_index: c_int = -1;
        let stream_count: c_int = (*format_context_ptr).nb_streams as c_int;

        // Prefer a stream explicitly tagged as English audio.
        let target_language: [&str; 3] = ["en", "eng", "english"];
        let target_media_type = AVMediaType::AVMEDIA_TYPE_AUDIO;

        for i in 0..stream_count {
            let stream: *mut AVStream = *(*format_context_ptr).streams.add(i as usize);
            let codec_par: *mut AVCodecParameters = (*stream).codecpar;

            if (*codec_par).codec_type != target_media_type {
                continue;
            }

            let lang_key = CString::new("language").unwrap();
            let entry = av_dict_get((*stream).metadata, lang_key.as_ptr(), ptr::null(), 0);
            if entry.is_null() {
                continue;
            }

            let lang = CStr::from_ptr((*entry).value).to_str().unwrap_or("");
            if target_language.contains(&lang) {
                selected_stream_index = i;
                break;
            }
        }

        if selected_stream_index < 0 {
            let best_stream_search = av_find_best_stream(
                format_context_ptr,
                target_media_type,
                -1,
                -1,
                ptr::null_mut(),
                0,
            );
            if best_stream_search < 0 {
                return Err(anyhow::anyhow!("Unable to find suitable audio stream!"));
            }
            selected_stream_index = best_stream_search;
        }

        let stream: *mut AVStream = *(*format_context_ptr)
            .streams
            .add(selected_stream_index as usize);
        let codec_par: *mut AVCodecParameters = (*stream).codecpar;

        // ---- decoder setup --------------------------------------------------
        let codec: *const AVCodec = avcodec_find_decoder((*codec_par).codec_id);
        if codec.is_null() {
            return Err(anyhow::anyhow!("Error finding the proper decoder codec!"));
        }

        let decoder_context_ptr: *mut AVCodecContext = avcodec_alloc_context3(codec);
        if decoder_context_ptr.is_null() {
            return Err(anyhow::anyhow!("Failed to allocate decoder context!"));
        }
        let decoder_guard = CodecContextGuard(decoder_context_ptr);

        if avcodec_parameters_to_context(decoder_context_ptr, codec_par) < 0 {
            return Err(anyhow::anyhow!(
                "Failed to copy codec parameters for decoder context"
            ));
        }
        if avcodec_open2(decoder_context_ptr, codec, ptr::null_mut()) < 0 {
            return Err(anyhow::anyhow!("Failed to initalize decoder context!"));
        }

        // ---- resampler setup -------------------------------------------------
        // Output: mono, 16 kHz, 32-bit float (old bitmask channel-layout API,
        // matching the AV_CH_LAYOUT_MONO import).
        let output_mono_channel_layout: AVChannelLayout = AVChannelLayout {
            order: AVChannelOrder::AV_CHANNEL_ORDER_NATIVE,
            nb_channels: 1,
            u: ffmpeg_sys_next::AVChannelLayout__bindgen_ty_1 {
                mask: AV_CH_LAYOUT_MONO,
            },
            opaque: std::ptr::null_mut(),
        };
        let output_channel_count = 1;
        let output_sample_rate: c_int = 16000;
        let output_sample_format = AVSampleFormat::AV_SAMPLE_FMT_FLT;

        // input channel_layout, sample rate, sample_format
        let mut input_channel_layout: AVChannelLayout = std::mem::zeroed();
        ffmpeg_sys_next::av_channel_layout_copy(&mut input_channel_layout, &(*codec_par).ch_layout);

        // CRITICAL FIX: Handle unspecified channel layout
        if input_channel_layout.order == AVChannelOrder::AV_CHANNEL_ORDER_UNSPEC {
            let nb_channels = (*codec_par).ch_layout.nb_channels;
            if nb_channels <= 0 {
                return Err(anyhow::anyhow!(
                    "Invalid number of channels in the target file!"
                ));
            }
            ffmpeg_sys_next::av_channel_layout_default(&mut input_channel_layout, nb_channels);
        }

        let input_sample_rate: c_int = (*codec_par).sample_rate;
        let input_sample_format: AVSampleFormat = (*decoder_context_ptr).sample_fmt;

        let mut swr_context: *mut SwrContext = swr_alloc();

        let swr_opts = swr_alloc_set_opts2(
            &mut swr_context,
            &output_mono_channel_layout,
            output_sample_format,
            output_sample_rate,
            &input_channel_layout,
            input_sample_format,
            input_sample_rate,
            0,
            ptr::null_mut(),
        );

        if swr_opts < 0 {
            return Err(anyhow::anyhow!("Failed to create the SWR context!"));
        }

        let swr_guard = SwrContextGuard(swr_context);

        let filter_size = CString::new("filter_size").unwrap();
        let phase_shift = CString::new("phase_shift").unwrap();
        let linear_interp = CString::new("linear_interp").unwrap();
        let exact_rational = CString::new("exact_rational").unwrap();

        av_opt_set_int(swr_context as *mut c_void, filter_size.as_ptr(), 64, 0);

        av_opt_set_int(swr_context as *mut c_void, phase_shift.as_ptr(), 10, 0);

        av_opt_set_int(swr_context as *mut c_void, linear_interp.as_ptr(), 0, 0);

        av_opt_set_int(swr_context as *mut c_void, exact_rational.as_ptr(), 1, 0);

        if ffmpeg_sys_next::swr_init(swr_context) < 0 {
            return Err(anyhow::anyhow!("Failed to initialize SWR context!"));
        }

        // ---- packet/frame scratch space --------------------------------------
        let packet_ptr: *mut AVPacket = av_packet_alloc();
        if packet_ptr.is_null() {
            return Err(anyhow::anyhow!("Failed to allocate packet context!"));
        }
        let packet_guard = PacketGuard(packet_ptr);

        let frame_ptr: *mut AVFrame = av_frame_alloc();
        if frame_ptr.is_null() {
            return Err(anyhow::anyhow!("Failed to allocate frame context!"));
        }
        let frame_guard = FrameGuard(frame_ptr);

        // Create the temp file in the same directory as the destination so
        // that `persist()` below is a same-volume rename (Windows/most OSes
        // cannot rename across drives/volumes).

        // let mut decoded_samples = 0i64;
        // let mut written_samples = 0i64;
        let mut total_bytes = 0u64;

        // ---- decode / resample / write loop ----------------------------------
        while av_read_frame(format_context_ptr, packet_guard.0) >= 0 {
            if (*packet_guard.0).stream_index != selected_stream_index {
                av_packet_unref(packet_guard.0);
                continue;
            }

            // FIX: Proper error handling
            let send_ret = avcodec_send_packet(decoder_context_ptr, packet_guard.0);
            if send_ret < 0 && send_ret != AVERROR(EAGAIN) {
                return Err(anyhow::anyhow!(
                    "Error transferring packets to decoder context!"
                ));
            }

            loop {
                let ret = avcodec_receive_frame(decoder_context_ptr, frame_guard.0);

                if ret == AVERROR(EAGAIN) || ret == AVERROR_EOF {
                    break;
                } else if ret < 0 {
                    return Err(anyhow::anyhow!(
                        "Error fetching decoded frames from decoder!"
                    ));
                }

                let nb_samples: c_int =
                    ffmpeg_sys_next::swr_get_out_samples(swr_context, (*frame_guard.0).nb_samples);
                if nb_samples < 0 {
                    return Err(anyhow::anyhow!("Failed to output sample counts"));
                }

                let align: c_int = 0;
                let mut linesize: c_int = 0;
                let mut audio_data: *mut *mut u8 = ptr::null_mut();

                let alloc_ret = av_samples_alloc_array_and_samples(
                    &mut audio_data,
                    &mut linesize,
                    output_channel_count,
                    nb_samples,
                    output_sample_format,
                    align,
                );
                if alloc_ret < 0 || audio_data.is_null() {
                    return Err(anyhow::anyhow!("Failed to allocate output sample buffer"));
                }
                let sample_buffer_guard = SampleBufferGuard(audio_data);

                //let in_data = (*frame_guard.0).data.as_ptr() as *const *const u8;
                let in_data = (*frame_guard.0).extended_data as *const *const u8;

                let total_output_samples: c_int = ffmpeg_sys_next::swr_convert(
                    swr_context,
                    audio_data,
                    nb_samples,
                    in_data,
                    (*frame_guard.0).nb_samples,
                );

                if total_output_samples > 0 {
                    let valid_bytes = total_output_samples as usize * std::mem::size_of::<f32>();
                    let raw_bytes = slice::from_raw_parts(*audio_data as *const u8, valid_bytes);

                    tmpfile
                        .write_all(raw_bytes)
                        .context("Failed writing to temp file")?;

                    total_bytes += raw_bytes.len() as u64;

                    // decoded_samples += (*frame_guard.0).nb_samples as i64;
                    // written_samples += total_output_samples as i64;
                }

                drop(sample_buffer_guard);
                av_frame_unref(frame_guard.0);
            }

            av_packet_unref(packet_guard.0);
        }

        // ---- flush any samples still buffered inside the resampler ------------
        // swr_convert keeps a small amount of history/delay internally; once
        // the demuxer has hit EOF we drain that remainder with a null input
        // (this is intentionally OUTSIDE the per-packet decode loop above,
        // since it reflects true end-of-stream, not "no output yet for this
        // frame").
        loop {
            let nb_samples = ffmpeg_sys_next::swr_get_out_samples(swr_context, 0);
            if nb_samples <= 0 {
                break;
            }

            let align: c_int = 0;
            let mut linesize: c_int = 0;
            let mut audio_data: *mut *mut u8 = ptr::null_mut();

            let alloc_ret = av_samples_alloc_array_and_samples(
                &mut audio_data,
                &mut linesize,
                output_channel_count,
                nb_samples,
                output_sample_format,
                align,
            );
            if alloc_ret < 0 || audio_data.is_null() {
                return Err(anyhow::anyhow!("Failed to allocate output sample buffer"));
            }
            let sample_buffer_guard = SampleBufferGuard(audio_data);

            let total_output_samples: c_int =
                ffmpeg_sys_next::swr_convert(swr_context, audio_data, nb_samples, ptr::null(), 0);

            if total_output_samples > 0 {
                let valid_bytes = total_output_samples as usize * std::mem::size_of::<f32>();
                let raw_bytes = slice::from_raw_parts(*audio_data as *const u8, valid_bytes);
                tmpfile
                    .write_all(raw_bytes)
                    .context("Failed writing to temp file")?;
            } else {
                // Nothing more to drain; avoid spinning forever if swr keeps
                // reporting a stale non-zero estimate.
                drop(sample_buffer_guard);
                break;
            }

            drop(sample_buffer_guard);
        }

        // Explicitly drop decode-side resources before the format context, and
        // flush + persist the output file, all while still inside `unsafe`
        // only where actually required.
        drop(frame_guard);
        drop(packet_guard);
        drop(swr_guard);
        drop(decoder_guard);
        drop(format_guard);

        let metadata = TranscoderMetadata {
            input_sample_rate: input_sample_rate as usize,
            resampled_file_size: total_bytes as usize,
        };

        return Ok(metadata);
    }
}

/*

println!("total_bytes = {}", total_bytes);
        println!(
            "tempfile bytes = {}",
            tmpfile.as_file().metadata().unwrap().len()
        );
        eprintln!(
            "Decoded Frames = {}, Written Frames = {}",
            decoded_samples, written_samples
        );

*/
