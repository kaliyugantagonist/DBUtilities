package com.scania.datalake.exception;

public class InvalidDBPropertyException extends Exception {

    public InvalidDBPropertyException() {
        super();
    }

    public InvalidDBPropertyException(String message) {
        super(message);
    }

    public InvalidDBPropertyException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
