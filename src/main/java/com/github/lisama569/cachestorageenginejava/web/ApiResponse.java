package com.github.lisama569.cachestorageenginejava.web;

/**
 * 统一响应包。
 */
public record ApiResponse<T>(boolean success, String code, T data, String message) {

    public static <T> ApiResponse<T> ok(T data) {
        return new ApiResponse<>(true, "OK", data, null);
    }

    public static <T> ApiResponse<T> fail(String code, String message) {
        return new ApiResponse<>(false, code, null, message);
    }
}
