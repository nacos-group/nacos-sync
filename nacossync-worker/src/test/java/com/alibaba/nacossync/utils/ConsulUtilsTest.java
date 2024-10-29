package com.alibaba.nacossync.utils;

import com.alibaba.nacossync.util.ConsulUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConsulUtilsTest {

    /**
     * Test the transferMetadata method with valid tags containing key-value pairs.
     * This test ensures that all valid tags are correctly parsed into the resulting metadata map.
     */
    @Test
    public void testTransferMetadata_withValidTags() {
        // Test with valid tags containing key-value pairs
        List<String> tags = Arrays.asList("key1=value1", "key2=value2");
        Map<String, String> expectedMetadata = new HashMap<>();
        expectedMetadata.put("key1", "value1");
        expectedMetadata.put("key2", "value2");

        Map<String, String> actualMetadata = ConsulUtils.transferMetadata(tags);

        // Assert that the actual metadata matches the expected metadata
        assertEquals(expectedMetadata, actualMetadata);
    }

    /**
     * Test the transferMetadata method with invalid tags that do not contain key-value pairs.
     * This test ensures that only valid key-value pairs are included in the resulting metadata map.
     */
    @Test
    public void testTransferMetadata_withInvalidTags() {
        // Test with tags that do not contain key-value pairs
        List<String> tags = Arrays.asList("key1", "key2=value2", "invalidTag");
        Map<String, String> expectedMetadata = new HashMap<>();
        expectedMetadata.put("key2", "value2");

        Map<String, String> actualMetadata = ConsulUtils.transferMetadata(tags);

        // Assert that the actual metadata matches the expected metadata
        assertEquals(expectedMetadata, actualMetadata);
    }

    /**
     * Test the transferMetadata method with an empty list of tags.
     * This test ensures that an empty list results in an empty metadata map.
     */
    @Test
    public void testTransferMetadata_withEmptyTags() {
        // Test with an empty list of tags
        List<String> tags = Collections.emptyList();
        Map<String, String> expectedMetadata = new HashMap<>();

        Map<String, String> actualMetadata = ConsulUtils.transferMetadata(tags);

        // Assert that the actual metadata is empty
        assertEquals(expectedMetadata, actualMetadata);
    }

    /**
     * Test the transferMetadata method with null tags.
     * This test ensures that a null input results in an empty metadata map.
     */
    @Test
    public void testTransferMetadata_withNullTags() {
        // Test with null tags
        Map<String, String> expectedMetadata = new HashMap<>();

        Map<String, String> actualMetadata = ConsulUtils.transferMetadata(null);

        // Assert that the actual metadata is empty
        assertEquals(expectedMetadata, actualMetadata);
    }
}
