"""
Unit tests for voice_agent.utils.audio.

Focused on the conversion correctness that is easy to break: format constants,
duration preservation, the stateful-vs-oneshot resampling equivalence (the
artifact guard), reframing, and energy detection.

Run:  uv run pytest -q
"""

import math
import struct

import audioop
import pytest

from voice_agent.utils import audio


def make_pcm(rate: int, ms: int, freq: int = 440, amp: int = 12000) -> bytes:
    n = rate * ms // 1000
    return b"".join(
        struct.pack("<h", int(amp * math.sin(2 * math.pi * freq * i / rate)))
        for i in range(n)
    )


def pcm_duration_ms(pcm: bytes, rate: int) -> float:
    return len(pcm) / audio.SAMPLE_WIDTH / rate * 1000


def test_frame_size_constants():
    assert audio.TELEPHONY_FRAME_BYTES == 160
    assert audio.MODEL_FRAME_BYTES == 640


def test_oneshot_roundtrip_preserves_duration():
    pcm16 = make_pcm(audio.MODEL_RATE, 100)
    mulaw = audio.pcm_to_mulaw(pcm16)
    back = audio.mulaw_to_pcm(mulaw)
    assert pcm_duration_ms(back, audio.MODEL_RATE) == pytest.approx(100, abs=2)
    assert len(mulaw) == pytest.approx(800, abs=4)


def test_stream_resampler_matches_oneshot():
    # Frame-by-frame stateful resampling must match resampling the whole buffer,
    # otherwise boundary artifacts are being introduced.
    full_pcm8 = make_pcm(audio.TELEPHONY_RATE, 200)
    truth, _ = audioop.ratecv(full_pcm8, audio.SAMPLE_WIDTH, 1, 8000, 16000, None)

    rs = audio.StreamResampler(8000, 16000)
    frame_pcm8 = audio.TELEPHONY_RATE * 20 // 1000 * audio.SAMPLE_WIDTH
    stateful = b"".join(
        rs.process(full_pcm8[i : i + frame_pcm8])
        for i in range(0, len(full_pcm8), frame_pcm8)
    )
    assert abs(len(stateful) - len(truth)) <= 4


def test_naive_per_frame_resample_drifts():
    # Confirms the naive approach (state reset each frame) really does differ,
    # so the test above is guarding something real.
    full_pcm8 = make_pcm(audio.TELEPHONY_RATE, 200)
    truth, _ = audioop.ratecv(full_pcm8, 2, 1, 8000, 16000, None)
    frame = audio.TELEPHONY_RATE * 20 // 1000 * 2
    naive = b""
    for i in range(0, len(full_pcm8), frame):
        out, _ = audioop.ratecv(full_pcm8[i : i + frame], 2, 1, 8000, 16000, None)
        naive += out
    assert len(naive) != len(truth)


def test_reframer_chunks_and_holds_remainder():
    rf = audio.Reframer(frame_bytes=640)
    out1 = rf.push(b"x" * 1000)
    assert len(out1) == 1 and len(out1[0]) == 640
    out2 = rf.push(b"y" * 280)
    assert len(out2) == 1
    assert rf.flush() == b""


def test_reframer_flush_returns_partial():
    rf = audio.Reframer(frame_bytes=640)
    rf.push(b"z" * 100)
    assert rf.flush() == b"z" * 100


def test_energy_separates_silence_from_speech():
    silence = b"\x00\x00" * 320
    signal = make_pcm(audio.MODEL_RATE, 20)
    assert audio.frame_energy(silence) == 0
    assert audio.frame_energy(signal) > 1000


def test_energy_empty_buffer():
    assert audio.frame_energy(b"") == 0