package evil.inc.kafkasandbox.payload.json;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@AllArgsConstructor
@RequiredArgsConstructor
public class Customer {
    private int id;
    private String customerName;
    private String faxNumber;
}
