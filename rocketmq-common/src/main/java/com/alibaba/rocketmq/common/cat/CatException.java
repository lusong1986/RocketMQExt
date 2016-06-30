package com.alibaba.rocketmq.common.cat;

public class CatException extends Exception {

	private static final long serialVersionUID = 7483143626733262802L;

	public CatException() {
		super();
	}

	public CatException(String message, Throwable cause) {
		super(message, cause);
	}

	public CatException(String message) {
		super(message);
	}

	public CatException(Throwable cause) {
		super(cause);
	}

}
