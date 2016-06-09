package com.researchworx.cresco.controller.shell;

import com.researchworx.cresco.controller.core.Launcher;
import org.apache.sshd.server.auth.password.PasswordAuthenticator;
import org.apache.sshd.server.session.ServerSession;

public class InAppPasswordAuthenticator implements PasswordAuthenticator {

    private Launcher plugin;
    public InAppPasswordAuthenticator(Launcher plugin) {
        this.plugin = plugin;
    }
    public boolean authenticate(String username, String password, ServerSession session) {

        boolean isAuth = false;
        String sshd_username = plugin.getConfig().getStringParam("sshd_username");
        String sshd_password = plugin.getConfig().getStringParam("sshd_password");

        if((sshd_username != null) && (sshd_password != null)) {
            if((username.equals(sshd_username)) && (password.equals(sshd_password))) {
                isAuth = true;
            }
        }
        else {
            isAuth = username != null && username.equals(password);
        }
        return isAuth;

    }
}