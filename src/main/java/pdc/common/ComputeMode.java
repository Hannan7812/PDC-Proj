package pdc.common;

public enum ComputeMode {
    WORD_COUNT,
    INVERTED_INDEX;

    public static ComputeMode from(String value) {
        return ComputeMode.valueOf(value.trim().toUpperCase());
    }
}
