package com.nablarch.example.redis;

import java.nio.charset.Charset;

public class NablarchRedisStringCodec implements NablarchRedisCodec<String> {
    private String charset;
    
    public NablarchRedisStringCodec() {
        this(Charset.defaultCharset().name());
    }
    
    public NablarchRedisStringCodec(String charset) {
        this.charset = charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    @Override
    public byte[] encode(String target) {
        return target == null ? null : target.getBytes(Charset.forName(charset));
    }

    @Override
    public String decode(byte[] bytes) {
        return bytes == null ? null : new String(bytes, Charset.forName(charset));
    }
}
