package io.github.mthyen.wmlens.event;

/**
 * Why a window fired - see {@link io.github.mthyen.wmlens.api.WatermarkLens#instrument}.
 */
public enum FiringType {
    /** The watermark passed the window's end - the window's first, normal firing. */
    ON_TIME,
    /** A late element arrived within {@code allowedLateness}, re-firing an already-fired window. */
    LATE_UPDATE
}
