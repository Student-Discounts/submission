package org.studentdiscounts.submission.config;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.springframework.web.filter.OncePerRequestFilter;
import tools.jackson.databind.ObjectMapper;

/**
 * Reads at most the configured cap plus one byte, including chunked bodies, before MVC can mutate
 * state.
 */
public final class RequestBodyLimitFilter extends OncePerRequestFilter {
  private final int maxBytes;
  private final ObjectMapper mapper;

  public RequestBodyLimitFilter(int maxBytes, ObjectMapper mapper) {
    if (maxBytes < 1 || maxBytes == Integer.MAX_VALUE)
      throw new IllegalArgumentException("Invalid body byte limit");
    this.maxBytes = maxBytes;
    this.mapper = mapper;
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    if (request.getContentLengthLong() > maxBytes) {
      tooLarge(request, response);
      return;
    }
    if (request.getContentLengthLong() == 0) {
      chain.doFilter(request, response);
      return;
    }
    byte[] body = request.getInputStream().readNBytes(maxBytes + 1);
    if (body.length > maxBytes) {
      tooLarge(request, response);
      return;
    }
    chain.doFilter(new BufferedRequest(request, body), response);
  }

  private void tooLarge(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    SecurityProblems.write(
        mapper,
        request,
        response,
        413,
        "BODY_TOO_LARGE",
        "Request body exceeds the configured byte limit");
  }

  private static final class BufferedRequest extends HttpServletRequestWrapper {
    private final byte[] body;
    private ServletInputStream stream;
    private BufferedReader reader;

    BufferedRequest(HttpServletRequest request, byte[] body) {
      super(request);
      this.body = body;
    }

    @Override
    public ServletInputStream getInputStream() {
      if (reader != null) throw new IllegalStateException("Reader has already been obtained");
      return stream();
    }

    private ServletInputStream stream() {
      if (stream == null)
        stream =
            new ServletInputStream() {
              private final ByteArrayInputStream input = new ByteArrayInputStream(body);

              @Override
              public int read() {
                return input.read();
              }

              @Override
              public int read(byte[] bytes, int offset, int length) {
                return input.read(bytes, offset, length);
              }

              @Override
              public int available() {
                return input.available();
              }

              @Override
              public boolean isFinished() {
                return input.available() == 0;
              }

              @Override
              public boolean isReady() {
                return true;
              }

              @Override
              public void setReadListener(ReadListener listener) {
                try {
                  if (!isFinished()) listener.onDataAvailable();
                  if (isFinished()) listener.onAllDataRead();
                } catch (IOException failure) {
                  listener.onError(failure);
                }
              }
            };
      return stream;
    }

    @Override
    public BufferedReader getReader() {
      if (stream != null && reader == null)
        throw new IllegalStateException("Input stream has already been obtained");
      if (reader == null) {
        String encoding = getCharacterEncoding();
        reader =
            new BufferedReader(
                new InputStreamReader(
                    stream(),
                    encoding == null
                        ? StandardCharsets.UTF_8
                        : java.nio.charset.Charset.forName(encoding)));
      }
      return reader;
    }
  }
}
