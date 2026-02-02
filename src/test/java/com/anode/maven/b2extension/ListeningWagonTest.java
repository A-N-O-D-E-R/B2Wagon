package com.anode.maven.b2extension;

import org.apache.maven.wagon.events.SessionEvent;
import org.apache.maven.wagon.events.SessionListener;
import org.apache.maven.wagon.events.TransferEvent;
import org.apache.maven.wagon.events.TransferListener;
import org.apache.maven.wagon.repository.Repository;
import org.apache.maven.wagon.resource.Resource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ListeningWagonTest {

    private TestableListeningWagon wagon;

    @Mock
    private SessionListener mockSessionListener;

    @Mock
    private TransferListener mockTransferListener;

    @Before
    public void setUp() {
        wagon = new TestableListeningWagon();
    }

    @Test
    public void testAddRemoveSessionListener() {
        assertFalse(wagon.hasSessionListener(mockSessionListener));

        wagon.addSessionListener(mockSessionListener);
        assertTrue(wagon.hasSessionListener(mockSessionListener));

        wagon.removeSessionListener(mockSessionListener);
        assertFalse(wagon.hasSessionListener(mockSessionListener));
    }

    @Test
    public void testAddRemoveTransferListener() {
        assertFalse(wagon.hasTransferListener(mockTransferListener));

        wagon.addTransferListener(mockTransferListener);
        assertTrue(wagon.hasTransferListener(mockTransferListener));

        wagon.removeTransferListener(mockTransferListener);
        assertFalse(wagon.hasTransferListener(mockTransferListener));
    }

    @Test
    public void testFireSessionOpening() {
        wagon.addSessionListener(mockSessionListener);
        wagon.testFireSessionOpening();

        ArgumentCaptor<SessionEvent> captor = ArgumentCaptor.forClass(SessionEvent.class);
        verify(mockSessionListener).sessionOpening(captor.capture());

        assertEquals(SessionEvent.SESSION_OPENING, captor.getValue().getEventType());
    }

    @Test
    public void testFireSessionOpened() {
        wagon.addSessionListener(mockSessionListener);
        wagon.testFireSessionOpened();

        ArgumentCaptor<SessionEvent> captor = ArgumentCaptor.forClass(SessionEvent.class);
        verify(mockSessionListener).sessionOpened(captor.capture());

        assertEquals(SessionEvent.SESSION_OPENED, captor.getValue().getEventType());
    }

    @Test
    public void testFireSessionDisconnecting() {
        wagon.addSessionListener(mockSessionListener);
        wagon.testFireSessionDisconnecting();

        verify(mockSessionListener).sessionDisconnecting(any(SessionEvent.class));
    }

    @Test
    public void testFireSessionDisconnected() {
        wagon.addSessionListener(mockSessionListener);
        wagon.testFireSessionDisconnected();

        verify(mockSessionListener).sessionDisconnected(any(SessionEvent.class));
    }

    @Test
    public void testFireGetTransferInitiated() {
        wagon.addTransferListener(mockTransferListener);
        Resource resource = new Resource("test.jar");
        File localFile = new File("/tmp/test.jar");

        wagon.testFireGetTransferInitiated(resource, localFile);

        ArgumentCaptor<TransferEvent> captor = ArgumentCaptor.forClass(TransferEvent.class);
        verify(mockTransferListener).transferInitiated(captor.capture());

        assertEquals(TransferEvent.TRANSFER_INITIATED, captor.getValue().getEventType());
        assertEquals(TransferEvent.REQUEST_GET, captor.getValue().getRequestType());
    }

    @Test
    public void testFirePutTransferInitiated() {
        wagon.addTransferListener(mockTransferListener);
        Resource resource = new Resource("test.jar");
        File localFile = new File("/tmp/test.jar");

        wagon.testFirePutTransferInitiated(resource, localFile);

        ArgumentCaptor<TransferEvent> captor = ArgumentCaptor.forClass(TransferEvent.class);
        verify(mockTransferListener).transferInitiated(captor.capture());

        assertEquals(TransferEvent.TRANSFER_INITIATED, captor.getValue().getEventType());
        assertEquals(TransferEvent.REQUEST_PUT, captor.getValue().getRequestType());
    }

    @Test
    public void testFireTransferCompleted() {
        wagon.addTransferListener(mockTransferListener);
        Resource resource = new Resource("test.jar");
        File localFile = new File("/tmp/test.jar");

        wagon.testFireGetTransferCompleted(resource, localFile);

        verify(mockTransferListener).transferCompleted(any(TransferEvent.class));
    }

    @Test
    public void testFireTransferProgress() {
        wagon.addTransferListener(mockTransferListener);
        Resource resource = new Resource("test.jar");
        File localFile = new File("/tmp/test.jar");
        byte[] buffer = "test data".getBytes();

        wagon.testFireGetTransferProgress(resource, localFile, buffer, buffer.length);

        verify(mockTransferListener).transferProgress(any(TransferEvent.class), eq(buffer), eq(buffer.length));
    }

    /**
     * A testable subclass that exposes protected methods for testing.
     */
    private static class TestableListeningWagon extends ListeningWagon {

        public void testFireSessionOpening() {
            fireSessionOpening();
        }

        public void testFireSessionOpened() {
            fireSessionOpened();
        }

        public void testFireSessionDisconnecting() {
            fireSessionDisconnecting();
        }

        public void testFireSessionDisconnected() {
            fireSessionDisconnected();
        }

        public void testFireGetTransferInitiated(Resource resource, File localFile) {
            fireGetTransferInitiated(resource, localFile);
        }

        public void testFirePutTransferInitiated(Resource resource, File localFile) {
            firePutTransferInitiated(resource, localFile);
        }

        public void testFireGetTransferCompleted(Resource resource, File localFile) {
            fireGetTransferCompleted(resource, localFile);
        }

        public void testFireGetTransferProgress(Resource resource, File destination, byte[] buffer, int length) {
            fireGetTransferProgress(resource, destination, buffer, length);
        }

        @Override
        public void connect(Repository repository, org.apache.maven.wagon.authentication.AuthenticationInfo authenticationInfo,
                           org.apache.maven.wagon.proxy.ProxyInfoProvider proxyInfoProvider) {
            setRepository(repository);
        }

        @Override
        public void disconnect() {
        }

        @Override
        public void get(String resourceName, File destination) {
        }

        @Override
        public boolean getIfNewer(String resourceName, File destination, long timestamp) {
            return false;
        }

        @Override
        public void put(File source, String destination) {
        }

        @Override
        public boolean resourceExists(String resourceName) {
            return false;
        }

        @Override
        public boolean supportsDirectoryCopy() {
            return false;
        }
    }
}
