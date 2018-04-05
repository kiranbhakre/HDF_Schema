/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.example;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Customer extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Customer\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"first_name\",\"type\":\"string\",\"doc\":\"First Name of the Customer\",\"default\":\"\"},{\"name\":\"last_name\",\"type\":\"string\",\"doc\":\"Last Name of the Customer\",\"default\":\"\"},{\"name\":\"height\",\"type\":\"float\",\"doc\":\"Height at the time of registration\",\"default\":0.0}],\"version\":1}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  /** First Name of the Customer */
  @Deprecated public java.lang.CharSequence first_name;
  /** Last Name of the Customer */
  @Deprecated public java.lang.CharSequence last_name;
  /** Height at the time of registration */
  @Deprecated public float height;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public Customer() {}

  /**
   * All-args constructor.
   */
  public Customer(java.lang.CharSequence first_name, java.lang.CharSequence last_name, java.lang.Float height) {
    this.first_name = first_name;
    this.last_name = last_name;
    this.height = height;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return first_name;
    case 1: return last_name;
    case 2: return height;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: first_name = (java.lang.CharSequence)value$; break;
    case 1: last_name = (java.lang.CharSequence)value$; break;
    case 2: height = (java.lang.Float)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'first_name' field.
   * First Name of the Customer   */
  public java.lang.CharSequence getFirstName() {
    return first_name;
  }

  /**
   * Sets the value of the 'first_name' field.
   * First Name of the Customer   * @param value the value to set.
   */
  public void setFirstName(java.lang.CharSequence value) {
    this.first_name = value;
  }

  /**
   * Gets the value of the 'last_name' field.
   * Last Name of the Customer   */
  public java.lang.CharSequence getLastName() {
    return last_name;
  }

  /**
   * Sets the value of the 'last_name' field.
   * Last Name of the Customer   * @param value the value to set.
   */
  public void setLastName(java.lang.CharSequence value) {
    this.last_name = value;
  }

  /**
   * Gets the value of the 'height' field.
   * Height at the time of registration   */
  public java.lang.Float getHeight() {
    return height;
  }

  /**
   * Sets the value of the 'height' field.
   * Height at the time of registration   * @param value the value to set.
   */
  public void setHeight(java.lang.Float value) {
    this.height = value;
  }

  /** Creates a new Customer RecordBuilder */
  public static com.example.Customer.Builder newBuilder() {
    return new com.example.Customer.Builder();
  }
  
  /** Creates a new Customer RecordBuilder by copying an existing Builder */
  public static com.example.Customer.Builder newBuilder(com.example.Customer.Builder other) {
    return new com.example.Customer.Builder(other);
  }
  
  /** Creates a new Customer RecordBuilder by copying an existing Customer instance */
  public static com.example.Customer.Builder newBuilder(com.example.Customer other) {
    return new com.example.Customer.Builder(other);
  }
  
  /**
   * RecordBuilder for Customer instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Customer>
    implements org.apache.avro.data.RecordBuilder<Customer> {

    private java.lang.CharSequence first_name;
    private java.lang.CharSequence last_name;
    private float height;

    /** Creates a new Builder */
    private Builder() {
      super(com.example.Customer.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.example.Customer.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.first_name)) {
        this.first_name = data().deepCopy(fields()[0].schema(), other.first_name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.last_name)) {
        this.last_name = data().deepCopy(fields()[1].schema(), other.last_name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.height)) {
        this.height = data().deepCopy(fields()[2].schema(), other.height);
        fieldSetFlags()[2] = true;
      }
    }
    
    /** Creates a Builder by copying an existing Customer instance */
    private Builder(com.example.Customer other) {
            super(com.example.Customer.SCHEMA$);
      if (isValidValue(fields()[0], other.first_name)) {
        this.first_name = data().deepCopy(fields()[0].schema(), other.first_name);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.last_name)) {
        this.last_name = data().deepCopy(fields()[1].schema(), other.last_name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.height)) {
        this.height = data().deepCopy(fields()[2].schema(), other.height);
        fieldSetFlags()[2] = true;
      }
    }

    /** Gets the value of the 'first_name' field */
    public java.lang.CharSequence getFirstName() {
      return first_name;
    }
    
    /** Sets the value of the 'first_name' field */
    public com.example.Customer.Builder setFirstName(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.first_name = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'first_name' field has been set */
    public boolean hasFirstName() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'first_name' field */
    public com.example.Customer.Builder clearFirstName() {
      first_name = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'last_name' field */
    public java.lang.CharSequence getLastName() {
      return last_name;
    }
    
    /** Sets the value of the 'last_name' field */
    public com.example.Customer.Builder setLastName(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.last_name = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'last_name' field has been set */
    public boolean hasLastName() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'last_name' field */
    public com.example.Customer.Builder clearLastName() {
      last_name = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'height' field */
    public java.lang.Float getHeight() {
      return height;
    }
    
    /** Sets the value of the 'height' field */
    public com.example.Customer.Builder setHeight(float value) {
      validate(fields()[2], value);
      this.height = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'height' field has been set */
    public boolean hasHeight() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'height' field */
    public com.example.Customer.Builder clearHeight() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    public Customer build() {
      try {
        Customer record = new Customer();
        record.first_name = fieldSetFlags()[0] ? this.first_name : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.last_name = fieldSetFlags()[1] ? this.last_name : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.height = fieldSetFlags()[2] ? this.height : (java.lang.Float) defaultValue(fields()[2]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}