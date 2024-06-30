package com.lht.lhtmq.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author Leo
 * @date 2024/06/30
 */
@Data
@AllArgsConstructor
public class Result<T> {
    private int code;
    private T data;

    public static Result ok() {
        return new Result(1, null);
    }

    public static Result ok(String msg) {
        return new Result(1, msg);
    }

    public static Result msg(String msg) {
        return new Result(1, LhtMessage.create(msg, null));
    }

}
