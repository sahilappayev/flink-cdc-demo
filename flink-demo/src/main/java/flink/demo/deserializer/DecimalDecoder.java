package flink.demo.deserializer;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Base64;

public class DecimalDecoder {

    private DecimalDecoder() {
    }

    public static BigDecimal decode(String base64, int scale) {
        byte[] bytes = Base64.getDecoder().decode(base64);
        BigInteger unscaled = new BigInteger(bytes);
        return new BigDecimal(unscaled, scale);
    }
}
