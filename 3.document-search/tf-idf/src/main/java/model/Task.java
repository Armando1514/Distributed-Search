package model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class Task implements Serializable {
    private final List<String> searchTems;
    private final List<String> documents;

    public Task(List<String> searchTems, List<String> documents) {
        this.searchTems = searchTems;
        this.documents = documents;
    }

    public List<String> getSearchTems() {
        return Collections.unmodifiableList(searchTems);
    }

    public List<String> getDocuments(){
        return Collections.unmodifiableList(documents);
    }

}
