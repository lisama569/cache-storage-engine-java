package com.github.lisama569.cachestorageenginejava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "cache.data-dir=target/it-data")
@AutoConfigureMockMvc
class CacheControllerTest {

    private static final Logger log = LoggerFactory.getLogger(CacheControllerTest.class);

    @Autowired
    private MockMvc mvc;

    @Test
    void restApiRoundTripAndErrorCodes() throws Exception {
        log.info("输入: PUT /cache/hello (body=world), 再 GET, 再 DELETE, 再 GET");
        mvc.perform(put("/cache/hello").contentType(MediaType.APPLICATION_OCTET_STREAM)
                .content("world")).andExpect(status().isOk());

        MvcResult got = mvc.perform(get("/cache/hello")).andExpect(status().isOk()).andReturn();
        assertEquals("world", got.getResponse().getContentAsString());

        mvc.perform(delete("/cache/hello")).andExpect(status().isOk());

        MvcResult missing = mvc.perform(get("/cache/hello")).andExpect(status().isNotFound()).andReturn();
        log.info("判定依据: 删除后 GET 应返回 404 且响应体 status=NOT_FOUND; 实际={}",
                missing.getResponse().getContentAsString());
        assertEquals("NOT_FOUND",
                com.jayway.jsonpath.JsonPath.read(missing.getResponse().getContentAsString(), "$.status"));
    }
}
