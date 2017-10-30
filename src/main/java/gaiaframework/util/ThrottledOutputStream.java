package gaiaframework.util;

import com.google.common.util.concurrent.RateLimiter;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

// previously used the guava Rate Limiter, but has an inherent bottleneck..
// implementing a custom rate limiter.

public final class ThrottledOutputStream extends FilterOutputStream {

    private final RateLimiter rateLimiter;

    public ThrottledOutputStream(OutputStream out, double bytesPerSecond) {
        super(out);
        this.rateLimiter = RateLimiter.create(bytesPerSecond);
    }

    public void setRate(double bytesPerSecond) {
        rateLimiter.setRate(bytesPerSecond);
    }

    @Override
    public void write(int b) throws IOException {
        rateLimiter.acquire();
        super.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        rateLimiter.acquire(b.length);
        super.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        rateLimiter.acquire(len);
        super.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        super.flush();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}