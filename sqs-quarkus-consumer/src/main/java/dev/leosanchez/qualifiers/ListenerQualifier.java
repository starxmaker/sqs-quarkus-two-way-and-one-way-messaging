package dev.leosanchez.qualifiers;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.enterprise.util.Nonbinding;
import javax.inject.Qualifier;

@Qualifier
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target({ java.lang.annotation.ElementType.TYPE, java.lang.annotation.ElementType.FIELD })
public @interface ListenerQualifier {
    // here we define the metadata we want to attach to the message polling and processing
    @Nonbinding String urlProperty() default "";
     // if we want to process the messages in parallel or in sequence
    @Nonbinding boolean parallelProcessing() default true;
    // the maximum number of messages the listener will handle per polling
    @Nonbinding int maxNumberOfMessagesPerProcessing() default 10; 
    // a way to ensure that each processing at least take some time 
    @Nonbinding int minProcessingMilliseconds() default 0; 
}
