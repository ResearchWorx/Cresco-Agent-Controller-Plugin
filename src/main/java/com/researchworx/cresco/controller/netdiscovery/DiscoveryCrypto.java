package com.researchworx.cresco.controller.netdiscovery;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by cody on 5/12/16.
 */
public class DiscoveryCrypto {

    private static final Logger logger = LoggerFactory.getLogger(DiscoveryCrypto.class);

    private static final String ALGORITHM = "AES";

    public DiscoveryCrypto() {}


    private static String hashString(String message, String algorithm) {

        try {
            MessageDigest digest = MessageDigest.getInstance(algorithm);
            byte[] hashedBytes = digest.digest(message.getBytes("UTF-8"));

            return convertByteArrayToHexString(hashedBytes);
        }
        catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return null;
    }

    private static String convertByteArrayToHexString(byte[] arrayBytes) {
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < arrayBytes.length; i++) {
            stringBuffer.append(Integer.toString((arrayBytes[i] & 0xff) + 0x100, 16)
                    .substring(1));
        }
        return stringBuffer.toString();
    }

    private Key generateKeyFromString(final String secKey) throws Exception {
        final String HashKey = hashString(secKey, "SHA-1");
        final byte[] keyVal = new BASE64Decoder().decodeBuffer(HashKey);
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
            logger.error(ex.getMessage());
            ex.printStackTrace();
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
            logger.error(ex.getMessage());
            ex.printStackTrace();
        }

        return decryptedValue;
    }
}
