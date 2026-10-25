package com.ordersystem.security;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class JwtTokenProviderTest {

    @Test
    void blankSecretFailsFast() {
        assertThrows(IllegalStateException.class, () -> new JwtTokenProvider("", 60_000));
        assertThrows(IllegalStateException.class, () -> new JwtTokenProvider(null, 60_000));
    }

    @Test
    void weakSecretFailsFastInsteadOfBeingPadded() {
        // Regression: the old code zero-padded short secrets to 32 bytes.
        assertThrows(IllegalStateException.class, () -> new JwtTokenProvider("short", 60_000));
    }

    @Test
    void nonBase64SecretOfSufficientLengthWorks() {
        // Regression: Decoders.BASE64.decode used to run outside the try block,
        // crashing startup for plain-text secrets from .env files.
        JwtTokenProvider provider = new JwtTokenProvider(
                "a-plain-text-secret-that-is-long-enough!!", 60_000);
        String token = provider.generateToken("alice");
        assertTrue(provider.validateToken(token));
        assertEquals("alice", provider.getUsernameFromToken(token));
    }

    @Test
    void base64SecretRoundTrips() {
        String secret = Base64.getEncoder().encodeToString(new byte[48]);
        JwtTokenProvider provider = new JwtTokenProvider(secret, 60_000);
        String token = provider.generateToken("bob");
        assertEquals("bob", provider.getUsernameFromToken(token));
        assertFalse(provider.validateToken(token + "tampered"));
    }
}
