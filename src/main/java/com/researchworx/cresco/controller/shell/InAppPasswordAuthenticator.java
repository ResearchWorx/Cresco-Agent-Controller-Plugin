package com.researchworx.cresco.controller.shell;

import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.session.ServerSession;

/**
 * Created by vcbumg2 on 5/26/16.
 */


public class InAppPasswordAuthenticator implements PasswordAuthenticator {
    public boolean authenticate(String username, String password, ServerSession session) {
        return username != null && username.equals(password);
    }
}