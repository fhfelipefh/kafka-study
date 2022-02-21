package com.fhfelipefh.kafka;

public class RandomData {


  private static final String[] NAMES = { "John", "Paul", "George", "Ringo", "Pete", "felipe", "teste", "name" };
  private static final String[] ADDRESSES = { "123 Main St", "456 Maple St", "789 Broadway", "987 Elm St" };
  private static final String[] PHONES = { "123-456-7890", "234-567-8901", "345-678-9012", "456-789-0123" };


  public static String getRandomName() {
    return NAMES[(int)(Math.random() * NAMES.length)];
  }

  public static String getRandomAddress() {
    return ADDRESSES[(int)(Math.random() * ADDRESSES.length)];
  }

  public static String getRandomPhone() {
    return PHONES[(int)(Math.random() * PHONES.length)];
  }

}
