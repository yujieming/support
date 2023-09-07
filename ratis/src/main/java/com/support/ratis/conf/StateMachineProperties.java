

package com.support.ratis.conf;

import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class StateMachineProperties {
    private static final Logger LOG = LoggerFactory.getLogger(StateMachineProperties.class);

    private final ConcurrentMap<String, Object> properties = new ConcurrentHashMap<>();

    /**
     * A new configuration.
     */
    public StateMachineProperties() {
    }

    /**
     * A new StateMachineProperties with the same settings cloned from another.
     *
     * @param other the RaftProperties from which to clone settings.
     */
    public StateMachineProperties(StateMachineProperties other) {
        this.properties.putAll(other.properties);
    }

    String getenv(String name) {
        return System.getenv(name);
    }

    String getProperty(String key) {
        return System.getProperty(key);
    }


    public void set(String name, Object value) {
        final String trimmed = Objects.requireNonNull(name, "name == null").trim();
        Objects.requireNonNull(value, () -> "value == null for " + trimmed);
        properties.put(trimmed, value);
    }


    /**
     * Unset a previously set property.
     */
    public void unset(String name) {
        properties.remove(name);
    }

    /**
     * Sets a property if it is currently unset.
     *
     * @param name  the property name
     * @param value the new value
     */
    public synchronized void setIfUnset(String name, String value) {
        if (get(name) == null) {
            set(name, value);
        }
    }

    public Object get(String name) {
        return properties.get(name);
    }

    public <T> T get(String name, Class<T> clazz) {
        Object value = properties.get(name);
        if (Objects.isNull(value)) {
            return null;
        }
        if (clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        return null;
    }
    public <T> T getObject(String name, Class<T> clazz) {
        Object value = properties.get(name);
        if (Objects.isNull(value)) {
            return null;
        }
        if (clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        return null;
    }

    public Object get(String name, Object defaultValue) {
        return properties.getOrDefault(name, defaultValue);
    }

    public <T> T get(String name, T defaultValue, Class<T> clazz) {
        Object value = get(name);
        if (Objects.isNull(value)) {
            return defaultValue;
        }
        if (clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        return defaultValue;
    }

    public <T> T getObject(String name, T defaultValue, Class<T> clazz) {
        Object value = get(name);
        if (Objects.isNull(value)) {
            return defaultValue;
        }
        if (clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        return defaultValue;
    }

    /**
     * Set the value of the <code>name</code> property to an <code>int</code>.
     *
     * @param name  property name.
     * @param value <code>int</code> value of the property.
     */
    public void setInt(String name, int value) {
        set(name, value);
    }


    /**
     * Get the value of the <code>name</code> property as a <code>long</code>.
     * If no such property exists, the provided default value is returned,
     * or if the specified value is not a valid <code>long</code>,
     * then an error is thrown.
     *
     * @param name         property name.
     * @param defaultValue default value.
     * @return property value as a <code>long</code>,
     * or <code>defaultValue</code>.
     * @throws NumberFormatException when the value is invalid
     */
    public long getLong(String name, long defaultValue) {
        return get(name, defaultValue, Long.class);
    }

    /**
     * @return property value; if it is not set, return the default value.
     */
    public File getFile(String name, File defaultValue) {
        return get(name, defaultValue, File.class);
    }

    public void setFile(String name, File value) {
        set(name, value);
    }

    /**
     * Set the value of the <code>name</code> property to a <code>long</code>.
     *
     * @param name  property name.
     * @param value <code>long</code> value of the property.
     */
    public void setLong(String name, long value) {
        set(name, value);
    }

    /**
     * Get the value of the <code>name</code> property as a <code>double</code>.
     * If no such property exists, the provided default value is returned,
     * or if the specified value is not a valid <code>double</code>,
     * then an error is thrown.
     *
     * @param name         property name.
     * @param defaultValue default value.
     * @return property value as a <code>double</code>,
     * or <code>defaultValue</code>.
     * @throws NumberFormatException when the value is invalid
     */
    public double getDouble(String name, double defaultValue) {
        return get(name, defaultValue, Double.class);
    }

    /**
     * Set the value of the <code>name</code> property to a <code>double</code>.
     *
     * @param name  property name.
     * @param value property value.
     */
    public void setDouble(String name, double value) {
        set(name, value);
    }

    /**
     * Get the value of the <code>name</code> property as a <code>boolean</code>.
     * If no such property is specified, or if the specified value is not a valid
     * <code>boolean</code>, then <code>defaultValue</code> is returned.
     *
     * @param name         property name.
     * @param defaultValue default value.
     * @return property value as a <code>boolean</code>,
     * or <code>defaultValue</code>.
     */
    public boolean getBoolean(String name, boolean defaultValue) {
        return get(name, defaultValue, Boolean.class);
    }

    /**
     * Set the value of the <code>name</code> property to a <code>boolean</code>.
     *
     * @param name  property name.
     * @param value <code>boolean</code> value of the property.
     */
    public void setBoolean(String name, boolean value) {
        set(name, value);
    }

    /**
     * Set the value of the <code>name</code> property to the given type. This
     * is equivalent to <code>set(&lt;name&gt;, value.toString())</code>.
     *
     * @param name  property name
     * @param value new value
     */
    public <T extends Enum<T>> void setEnum(String name, T value) {
        set(name, value.toString());
    }

    /**
     * Return value matching this enumerated type.
     * Note that the returned value is trimmed by this method.
     *
     * @param name         Property name
     * @param defaultValue Value returned if no mapping exists
     * @throws IllegalArgumentException If mapping is illegal for the type
     *                                  provided
     */
    public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
        final String val = get(name, null, String.class);
        return null == val
                ? defaultValue
                : Enum.valueOf(defaultValue.getDeclaringClass(), val);
    }

    /**
     * Set the value of <code>name</code> to the given time duration. This
     * is equivalent to <code>set(&lt;name&gt;, value + &lt;time suffix&gt;)</code>.
     *
     * @param name  Property name
     * @param value Time duration
     */
    public void setTimeDuration(String name, TimeDuration value) {
        set(name, value);
    }

    /**
     * Return time duration in the given time unit. Valid units are encoded in
     * properties as suffixes: nanoseconds (ns), microseconds (us), milliseconds
     * (ms), seconds (s), minutes (m), hours (h), and days (d).
     *
     * @param name         Property name
     * @param defaultValue Value returned if no mapping exists.
     * @throws NumberFormatException If the property stripped of its unit is not
     *                               a number
     */
    public TimeDuration getTimeDuration(
            String name, TimeDuration defaultValue) {
        return get(name, defaultValue, TimeDuration.class);
    }

    /**
     * Get the value of the <code>name</code> property as a <code>Class</code>
     * implementing the interface specified by <code>xface</code>.
     * <p>
     * If no such property is specified, then <code>defaultValue</code> is
     * returned.
     * <p>
     * An exception is thrown if the returned class does not implement the named
     * interface.
     *
     * @param name         the class name.
     * @param defaultValue default value.
     * @param xface        the interface implemented by the named class.
     * @return property value as a <code>Class</code>,
     * or <code>defaultValue</code>.
     */
    public <BASE> Class<? extends BASE> getClass(
            String name, Class<? extends BASE> defaultValue, Class<BASE> xface) {
        try {
            Class clazz = (Class) get(name, defaultValue);
            if (clazz != null && !xface.isAssignableFrom(clazz)) {
                throw new RuntimeException(clazz + " not " + xface.getName());
            } else if (clazz != null) {
                return clazz.asSubclass(xface);
            } else {
                return null;
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set the value of the <code>name</code> property to the name of a
     * <code>theClass</code> implementing the given interface <code>xface</code>.
     * <p>
     * An exception is thrown if <code>theClass</code> does not implement the
     * interface <code>xface</code>.
     *
     * @param name     property name.
     * @param theClass property value.
     * @param xface    the interface implemented by the named class.
     */
    public void setClass(String name, Class<?> theClass, Class<?> xface) {
        if (!xface.isAssignableFrom(theClass)) {
            throw new RuntimeException(theClass + " not " + xface.getName());
        }
        set(name, theClass);
    }

    /**
     * @return number of keys in the properties.
     */
    public int size() {
        return properties.size();
    }

    /**
     * Clears all keys from the configuration.
     */
    public void clear() {
        properties.clear();
    }

    @Override
    public String toString() {
        return JavaUtils.getClassSimpleName(getClass()) + ":" + size();
    }
}
