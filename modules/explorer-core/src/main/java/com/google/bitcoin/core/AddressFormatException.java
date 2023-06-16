package com.google.bitcoin.core;

public class AddressFormatException extends Exception {
    public AddressFormatException() {
        super();
    }

    public AddressFormatException(String message) {
        super(message);
    }
}