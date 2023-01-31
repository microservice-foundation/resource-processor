package com.epam.training.microservicefoundation.resourceprocessor.model;

public class ResourceRecordTemp {
    private final long id;
    private final String name;
    private final String path;

    public ResourceRecordTemp(long id, String name, String path) {
        this.id = id;
        this.name = name;
        this.path = path;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "ResourceRecord{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", path='" + path + '\'' +
                '}';
    }
}
