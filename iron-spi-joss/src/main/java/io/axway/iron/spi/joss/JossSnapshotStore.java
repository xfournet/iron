package io.axway.iron.spi.joss;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.stream.*;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;
import io.axway.iron.spi.storage.SnapshotStore;

class JossSnapshotStore implements SnapshotStore {
    private static final String FILENAME_FORMAT = "%020d.tx";
    private static final Pattern FILENAME_PATTERN = Pattern.compile("([0-9]{20})\\.tx");

    private final Container m_container;

    JossSnapshotStore(Account account, String storeName) {
        m_container = account.getContainer(storeName);
        if (!m_container.exists()) {
            m_container.create();
        }
    }

    @Override
    public OutputStream createSnapshotWriter(long transactionId) throws IOException {
        StoredObject object = getTxObject(transactionId);
        if (object.exists()) {
            throw new RuntimeException("Object already exists: " + object.getPath()); // TODO better exception
        }

        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                object.uploadObject(toByteArray());
            }
        };
    }

    @Override
    public InputStream createSnapshotReader(long transactionId) throws IOException {
        StoredObject object = getTxObject(transactionId);
        if (!object.exists()) {
            throw new RuntimeException("Object already doesn't exist: " + object.getPath()); // TODO better exception
        }

        return object.downloadObjectAsInputStream();
    }

    @Override
    public List<Long> listSnapshots() {
        return m_container.list().stream() //
                .map(storedObject -> FILENAME_PATTERN.matcher(storedObject.getPath())) //
                .filter(Matcher::matches) //
                .map(matcher -> matcher.group(1)) //
                .map(Long::parseLong) //
                .collect(Collectors.toList());
    }

    @Override
    public void deleteSnapshot(long transactionId) {
        StoredObject object = getTxObject(transactionId);
        if (object.exists()) {
            object.delete();
        }
        // ignore case when the snapshot to delete doesn't exist
    }

    private StoredObject getTxObject(long txId) {
        String objectName = String.format(FILENAME_FORMAT, txId);
        return m_container.getObject(objectName);
    }
}
