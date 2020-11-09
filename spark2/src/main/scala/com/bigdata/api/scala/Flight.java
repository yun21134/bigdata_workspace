package com.bigdata.api.scala;



import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

public class Flight implements Serializable {
    @Getter
    @Setter
    String DEST_COUNTRY_NAME;

    @Getter
    @Setter
    String ORIGIN_COUNTRY_NAME;

    @Getter
    @Setter
    Long count;

    @Override
    public String toString() {
        return getDEST_COUNTRY_NAME() + ", "
                + getORIGIN_COUNTRY_NAME() + ", "
                + getCount();


    }
}
