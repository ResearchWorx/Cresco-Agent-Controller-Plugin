package com.researchworx.cresco.controller.shell;

import org.apache.sshd.server.keyprovider.AbstractGeneratorHostKeyProvider;

import java.io.IOException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.nio.file.Files;
import java.security.PublicKey;

import org.apache.sshd.common.config.keys.KeyUtils;
import org.apache.sshd.common.util.io.IoUtils;
/**
 * Created by vcbumg2 on 5/26/16.
 */

public abstract class CrescoHostKeyProvider extends AbstractGeneratorHostKeyProvider {

    protected KeyPair resolveKeyPair(Path keyPath) throws IOException, GeneralSecurityException {
        if (keyPath != null) {
            LinkOption[] options = IoUtils.getLinkOptions(false);
            if (Files.exists(keyPath, options) && Files.isRegularFile(keyPath, options)) {
                try {
                    KeyPair kp = readKeyPair(keyPath, IoUtils.EMPTY_OPEN_OPTIONS);

                    if (kp != null) {
                        if (log.isDebugEnabled()) {
                            PublicKey key = kp.getPublic();
                            log.debug("resolveKeyPair({}) loaded key={}-{}",
                                    keyPath, KeyUtils.getKeyType(key), KeyUtils.getFingerPrint(key));
                        }
                        return kp;
                    }
                } catch (Throwable e) {
                    log.warn("resolveKeyPair({}) Failed ({}) to load: {}",
                            keyPath, e.getClass().getSimpleName(), e.getMessage());
                    if (log.isDebugEnabled()) {
                        log.debug("resolveKeyPair(" + keyPath + ") load failure details", e);
                    }
                }
            }
        }

        // either no file specified or no key in file
        String alg = getAlgorithm();
        KeyPair kp;
        try {
            kp = generateKeyPair(alg);
            if (kp == null) {
                return null;
            }

            if (log.isDebugEnabled()) {
                PublicKey key = kp.getPublic();
                log.debug("resolveKeyPair({}) generated {} key={}-{}",
                        keyPath, alg, KeyUtils.getKeyType(key), KeyUtils.getFingerPrint(key));
            }
        } catch (Throwable e) {
            log.warn("resolveKeyPair({})[{}] Failed ({}) to generate {} key-pair: {}",
                    keyPath, alg, e.getClass().getSimpleName(), alg, e.getMessage());
            if (log.isDebugEnabled()) {
                log.debug("resolveKeyPair(" + keyPath + ")[" + alg + "] key-pair generation failure details", e);
            }

            return null;
        }

        if (keyPath != null) {
            try {
                writeKeyPair(kp, keyPath);
            } catch (Throwable e) {
                log.warn("resolveKeyPair({})[{}] Failed ({}) to write {} key: {}",
                        alg, keyPath, e.getClass().getSimpleName(), alg, e.getMessage());
                if (log.isDebugEnabled()) {
                    log.debug("resolveKeyPair(" + keyPath + ")[" + alg + "] write failure details", e);
                }
            }
        }

        return kp;
    }


}