package org.studentdiscounts.submission.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import tools.jackson.databind.json.JsonMapper;

class RequestFiltersTest {
  final JsonMapper mapper = JsonMapper.builder().build();

  @Test
  void enforcesKnownAndChunkedBodyCapsBeforeControllerRuns() throws Exception {
    for (boolean chunked : new boolean[] {false, true}) {
      var request =
          new MockHttpServletRequest("POST", "/api/v1/submissions") {
            @Override
            public int getContentLength() {
              return chunked ? -1 : super.getContentLength();
            }

            @Override
            public long getContentLengthLong() {
              return chunked ? -1 : super.getContentLengthLong();
            }
          };
      request.setContent("12345".getBytes(StandardCharsets.UTF_8));
      var response = new MockHttpServletResponse();
      boolean[] called = {false};
      new RequestBodyLimitFilter(4, mapper)
          .doFilter(request, response, (req, res) -> called[0] = true);
      assertThat(response.getStatus()).isEqualTo(413);
      assertThat(response.getContentType()).startsWith("application/problem+json");
      assertThat(called[0]).isFalse();
    }
  }

  @Test
  void replaysExactlyBoundedBodyAndPreservesUtf8Reader() throws Exception {
    var request = new MockHttpServletRequest("PATCH", "/api/v1/submissions/123");
    request.setCharacterEncoding("UTF-8");
    request.setContent("кофе".getBytes(StandardCharsets.UTF_8));
    var response = new MockHttpServletResponse();
    new RequestBodyLimitFilter(8, mapper)
        .doFilter(
            request,
            response,
            (req, res) -> {
              assertThat(req.getReader().readLine()).isEqualTo("кофе");
            });
    assertThat(response.getStatus()).isEqualTo(200);
  }

  @Test
  void appliesExact256KibBoundaryForUnknownLengthBodies() throws Exception {
    for (int size : new int[] {262144, 262145}) {
      var request =
          new MockHttpServletRequest("POST", "/api/v1/submissions") {
            @Override
            public long getContentLengthLong() {
              return -1;
            }
          };
      request.setContent(new byte[size]);
      var response = new MockHttpServletResponse();
      boolean[] called = {false};
      new RequestBodyLimitFilter(262144, mapper)
          .doFilter(
              request,
              response,
              (req, res) -> {
                called[0] = true;
                assertThat(req.getInputStream().readAllBytes()).hasSize(size);
              });
      assertThat(called[0]).isEqualTo(size == 262144);
      assertThat(response.getStatus()).isEqualTo(size == 262144 ? 200 : 413);
    }
  }

  @Test
  void propagatesSafeCorrelationAndRestoresCallingMdcOnExceptions() throws Exception {
    MDC.put("correlationId", "outer");
    var request = new MockHttpServletRequest("GET", "/api/v1/submissions");
    request.addHeader("X-Correlation-ID", "case-123_ABC.7");
    var response = new MockHttpServletResponse();
    try {
      new CorrelationFilter()
          .doFilter(
              request,
              response,
              (req, res) -> {
                assertThat(MDC.get("correlationId")).isEqualTo("case-123_ABC.7");
                assertThat(req.getAttribute("correlationId")).isEqualTo("case-123_ABC.7");
                throw new IOException("synthetic");
              });
    } catch (IOException expected) {
      assertThat(expected.getMessage()).isEqualTo("synthetic");
    }
    assertThat(response.getHeader("X-Correlation-ID")).isEqualTo("case-123_ABC.7");
    assertThat(MDC.get("correlationId")).isEqualTo("outer");
    MDC.clear();
  }

  @Test
  void replacesUnsafeCorrelationInputWithGeneratedIdentifier() throws Exception {
    for (String header : new String[] {"", "x".repeat(65), "unsafe space", "<script>"}) {
      var request = new MockHttpServletRequest();
      request.addHeader("X-Correlation-ID", header);
      var response = new MockHttpServletResponse();
      new CorrelationFilter().doFilter(request, response, (req, res) -> {});
      assertThat(response.getHeader("X-Correlation-ID")).matches("[A-Za-z0-9._-]{1,64}");
      assertThat(response.getHeader("X-Correlation-ID")).isNotEqualTo(header);
    }
    assertThat(MDC.get("correlationId")).isNull();
  }
}
