package org.apache.hadoop.examples;

public class A {
	public void fun1() {
		this.fun2();
		this.fun3();
	}
	
	public void fun2() {
		System.out.println("A“Ifun2");
	}
	public void fun3() {
		System.out.println("A“Ifun3");
	}

}
