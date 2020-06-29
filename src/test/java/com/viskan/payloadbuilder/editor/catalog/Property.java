package com.viskan.payloadbuilder.editor.catalog;

import java.beans.PropertyChangeSupport;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Property annotation. Placed on catalog properties for UI to act upon */
@Target(value = ElementType.METHOD)
@Retention(value = RetentionPolicy.RUNTIME)
public @interface Property
{
    /** Sort of property */
    int sort() default 0;

    /** Title of property */
    String title();

    /** Tooltip of property */
    String tooltip() default "";

    /** Property presentation */
    Presentation presentation() default Presentation.STRING;

    /** Name of property. Used in {@link PropertyChangeSupport} */
    String name();

    /** Name of property to listen for when {@link #presentation()} is of type {@link Presentation#LIST}. Used in {@link PropertyChangeSupport} */
    String itemsPropertyName() default "";

    /** Number of items to show */
    int itemsListSize() default 20;

    /**
     * Group name of this item. If more than one group exists a tabbed pane will be created with the corresponding items
     */
    String group() default "";

    /** Presentation */
    enum Presentation
    {
        STRING,
        PASSWORD,
        INTEGER,
        LIST,
        ACTION;
    }

}
