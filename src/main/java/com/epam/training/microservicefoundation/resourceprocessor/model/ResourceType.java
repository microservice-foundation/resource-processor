package com.epam.training.microservicefoundation.resourceprocessor.model;

import java.util.Arrays;

public enum ResourceType {
    MP3(".mp3", "audio/mpeg");

    ResourceType(String extension, String mimeType) {
        this.extension = extension;
        this.mimeType = mimeType;
    }

    private final String extension;
    private final String mimeType;

    public String getExtension() {
        return extension;
    }

    public String getMimeType() {
        return mimeType;
    }

    public static ResourceType getResourceTypeByExtension(String extension) {
        return Arrays.stream(ResourceType.values())
                .filter(resourceType -> resourceType.extension.equals(extension))
                .findFirst()
                .orElse(null);
    }

    public static ResourceType getResourceTypeByMimeType(String mimeType) {
        return Arrays.stream(ResourceType.values())
                .filter(resourceType -> resourceType.mimeType.equals(mimeType))
                .findFirst()
                .orElse(null);
    }
}
