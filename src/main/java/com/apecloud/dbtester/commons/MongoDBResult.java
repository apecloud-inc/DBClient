package com.apecloud.dbtester.commons;

import com.mongodb.client.FindIterable;
import org.bson.Document;
import java.util.List;

public interface MongoDBResult extends QueryResult {
    List<Document> getDocuments();
    FindIterable<Document> getMongoResultSet();
}
