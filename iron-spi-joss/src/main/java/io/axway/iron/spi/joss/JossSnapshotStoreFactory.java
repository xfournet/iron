package io.axway.iron.spi.joss;

import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.model.Account;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.SnapshotStoreFactory;

public class JossSnapshotStoreFactory implements SnapshotStoreFactory {
    private final Account m_account;

    public JossSnapshotStoreFactory(AccountConfig accountConfig) {
        m_account = new AccountFactory(accountConfig).createAccount();
    }

    @Override
    public SnapshotStore createSnapshotStore(String storeName) {
        return new JossSnapshotStore(m_account, storeName);
    }
}
