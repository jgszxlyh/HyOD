package util;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface  leveretconeyTest{
    int times() default 1;
    boolean outputTime() default true;
    boolean enabled() default true;
    CreateInstanceTiming createInstanceTiming() default CreateInstanceTiming.CALL;


    public enum CreateInstanceTiming{
        CLASS,METHOD,CALL
    }
}
