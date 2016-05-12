package com.researchworx.cresco.controller.netdiscovery;

import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.security.Key;

/**
 * Created by cody on 5/12/16.
 */
public class DiscoveryCrypto {

    private static final String ALGORITHM = "AES";


    private Key generateKeyFromString(final String secKey) throws Exception {
        final byte[] keyVal = new BASE64Decoder().decodeBuffer(secKey);
        final Key key = new SecretKeySpec(keyVal, ALGORITHM);
        return key;
    }

    public String encrypt(final String valueEnc, final String secKey) {

        String encryptedValue = null;

        try {
            final Key key = generateKeyFromString(secKey);
            final Cipher c = Cipher.getInstance(ALGORITHM);
            c.init(Cipher.ENCRYPT_MODE, key);
            final byte[] encValue = c.doFinal(valueEnc.getBytes());
            encryptedValue = new BASE64Encoder().encode(encValue);
        } catch(Exception ex) {
            System.out.println("The Exception is=" + ex);
        }

        return encryptedValue;
    }

    public String decrypt(final String encryptedValue, final String secretKey) {

        String decryptedValue = null;

        try {

            final Key key = generateKeyFromString(secretKey);
            final Cipher c = Cipher.getInstance(ALGORITHM);
            c.init(Cipher.DECRYPT_MODE, key);
            final byte[] decorVal = new BASE64Decoder().decodeBuffer(encryptedValue);
            final byte[] decValue = c.doFinal(decorVal);
            decryptedValue = new String(decValue);
        } catch(Exception ex) {
            System.out.println("The Exception is=" + ex);
        }

        return decryptedValue;
    }
}
