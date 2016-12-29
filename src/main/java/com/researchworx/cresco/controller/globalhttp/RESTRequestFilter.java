package com.researchworx.cresco.controller.globalhttp;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.Principal;
import java.util.Map;
import java.util.logging.Logger;

@Provider
@PreMatching
public class RESTRequestFilter implements ContainerRequestFilter {

    private final static Logger log = Logger.getLogger( RESTRequestFilter.class.getName() );

    
    @Override
    public void filter( ContainerRequestContext requestCtx ) throws IOException 
    {
    	
    	//SecurityContext sc = requestCtx.getSecurityContext();
    	//sc.
    	
    	requestCtx.setSecurityContext(new SecurityContext() {
    		 
    		@Override
    		  public Principal getUserPrincipal() {
    		    //if (this.user == null) {
    		     // return requestCtx.getUserPrincipal();
    		    //}
    		     
    		    // make an anonymous implementation to just return our user
    		    return new Principal() {
    		      @Override
    		      public String getName() {   
    		    	  System.out.println("Is SMITH");
    		        return "smith";
    		      }
    		    };
    		  }
 
            @Override
            public boolean isUserInRole(final String role) 
            {
                //...
            	System.out.println("Is Role " + role);
            	return true;
            }
 
            @Override
            public boolean isSecure() 
            {
               // ...
            	System.out.println("Is Secure");
            	return true;
            }
 
            @Override
            public String getAuthenticationScheme() 
            {
                //...
            	return "SRv1";
            }
        });

        String path = requestCtx.getUriInfo().getPath();
        //log.info( "Filtering request path: " + path );

        // IMPORTANT!!! First, Acknowledge any pre-flight test from browsers for this case before validating the headers (CORS stuff)
        if ( requestCtx.getRequest().getMethod().equals( "OPTIONS" ) ) {
            requestCtx.abortWith( Response.status( Response.Status.OK ).build() );

            return;
        }
        
        //catch favicon
        if ( path.startsWith( "favicon.ico" )) 
        {
        	requestCtx.abortWith( Response.status( Response.Status.NOT_FOUND ).build() );
        	return;
        }
        
        // For any pther methods besides login, the authToken must be verified
        if ( (path.startsWith( "API/login" )) || (path.startsWith( "login.html" ))) 
        {
        	Authenticator demoAuthenticator = Authenticator.getInstance();
        	
        	//Check cookie first... then check if it is programatic
        	//requestCtx.getCookies()
        	System.out.println("LOGIN W0");
        	Map<String, Cookie> map=requestCtx.getCookies();
        	System.out.println("LOGIN W1");
        	String usercookie = null;
        	if(map.containsKey("SR_SESSION"))
        	{
        		usercookie = map.get("SR_SESSION").getValue();
        	}
        	System.out.println("LOGIN W2");
        	
        	if(demoAuthenticator.isCookieValid(usercookie))
        	{
        		System.out.println("LOGIN W3");
        		
        		System.out.println("Valid Cookie: " + usercookie);
            }
        }
        else if (path.startsWith( "PLUGINS" ))
        {
            //do nothing
        }
        else
        {
        	Authenticator demoAuthenticator = Authenticator.getInstance();
        	
        	//Check cookie first... then check if it is programatic
        	//requestCtx.getCookies()
        	Map<String, Cookie> map=requestCtx.getCookies();
        	String usercookie = null;
        	if(map.containsKey("SR_SESSION"))
        	{
        		usercookie = map.get("SR_SESSION").getValue();
        	}

        		//System.out.println("Usercookie: " + usercookie);
            	if(demoAuthenticator.isCookieValid(usercookie))
            	{
            		System.out.println("Using Cookie Auth");
            		
            		System.out.println("Valid Cookie: " + usercookie);
                }
            	else
            	{

					//redirect to login page
					if((path.endsWith(".html")))
					//if((path.endsWith(".html")) || (path.endsWith("")))
            		{

						URI targetURIForRedirection = null;
						try {
							targetURIForRedirection = new URI("login.html");
						} catch (URISyntaxException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
            			requestCtx.abortWith(Response.temporaryRedirect(targetURIForRedirection).build() ); 
            			return;
            			
            			
            		}


            		// Then check is the service key exists and is valid.
            		String serviceKey = requestCtx.getHeaderString( HTTPHeaderNames.SERVICE_KEY );


					if ( !demoAuthenticator.isServiceKeyValid( serviceKey ) )
            		{
						// Kick anyone without a valid service key
            			requestCtx.abortWith( Response.status( Response.Status.UNAUTHORIZED ).build() );
            			return;
            		}
					//for now just worry about servicekey
					/*
            		String authToken = requestCtx.getHeaderString( HTTPHeaderNames.AUTH_TOKEN );

            		// if it isn't valid, just kick them out.
            		if ( !demoAuthenticator.isAuthTokenValid( serviceKey, authToken ) ) 
            		{
            			requestCtx.abortWith( Response.status( Response.Status.UNAUTHORIZED ).build() );
            		}
            		*/
            	}
            }
        }
    
}