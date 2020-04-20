package com.nablarch.example.redis;

import java.nio.charset.Charset;

public class NablarchRedisStringCodec implements NablarchRedisCodec<String> {
    private String charset;

    public void setCharset(String charset) {
        this.charset = charset;
    }

    @Override
    public byte[] encode(String target) {
        return target.getBytes(Charset.forName(charset));
    }

    @Override
    public String decode(byte[] bytes) {
        return new String(bytes, Charset.forName(charset));
    }
}
