package com.scania.datalake;

import javax.security.auth.Subject;
import java.security.AccessController;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        System.out.println( "Hello World! ");
        Subject activeSubject = Subject.getSubject(AccessController.getContext());
        System.out.println("Subject : "+activeSubject);

    }
}
