@Service
public class DeclarativeHoldPatternWithRetry {
    
    private static final int MAX_READ_RETRIES = 3;
    private static final int RETRY_DELAY_MS = 2000;
    
    @Autowired
    private JdbcTemplate writeJdbcTemplate;
    
    @Autowired
    private JdbcTemplate readJdbcTemplate;
    
    @Transactional(
        transactionManager = "writeDbTxManager",
        timeout = 600,  // 10 minutes to account for retries
        rollbackFor = Exception.class,
        isolation = Isolation.READ_COMMITTED
    )
    public void processBatch(List<String> paymentRefs) {
        
        // Step 1: Write payments
        Map<String, Long> refToIdMap = writePayments(paymentRefs);
        log.info("Wrote {} payments in open transaction", refToIdMap.size());
        
        // Step 2: Read with retry
        Map<String, PaymentDetail> paymentDetails = 
            readPaymentDetailsWithRetry(paymentRefs);
        
        // Step 3: Enrich payments
        enrichPayments(refToIdMap, paymentDetails);
        
        log.info("Successfully processed {} payments", paymentRefs.size());
		// Transaction automatically commits here (or rolls back if exception thrown)
    }
    
       @Retryable(
        value = {DataAccessException.class},
        maxAttempts = 3,
        backoff = @Backoff(delay = 2000, multiplier = 2)
    )
    private Map<String, PaymentDetail> readPaymentDetailsWithRetry(
            List<String> paymentRefs) {
        
        log.info("Attempting to read payment details...");
        
        Map<String, PaymentDetail> details = readPaymentDetailsByRef(paymentRefs);
        
        if (details.size() != paymentRefs.size()) {
            Set<String> missing = new HashSet<>(paymentRefs);
            missing.removeAll(details.keySet());
            throw new PaymentDetailsNotFoundException(
                "Missing details for: " + missing
            );
        }
        
        return details;
    }
    
    @Recover
    private Map<String, PaymentDetail> recoverFromReadFailure(
            DataAccessException e, List<String> paymentRefs) {
        
        log.error("All retry attempts failed for reading payment details", e);
        throw new PaymentBatchException(
            "Failed to read payment details after retries", e
        );
    }
    
    // Other methods same as before...
}
    private Map<String, Long> writePayments(List<String> paymentRefs) {
        // Same as before
        Map<String, Long> refToIdMap = new HashMap<>();
        
        for (String paymentRef : paymentRefs) {
            KeyHolder keyHolder = new GeneratedKeyHolder();
            
            writeJdbcTemplate.update(connection -> {
                PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO table1 (payment_ref, status, created_at) " +
                    "VALUES (?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS
                );
                ps.setString(1, paymentRef);
                ps.setString(2, "PENDING");
                ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                return ps;
            }, keyHolder);
            
            refToIdMap.put(paymentRef, keyHolder.getKey().longValue());
        }
        
        return refToIdMap;
    }
    
    private Map<String, PaymentDetail> readPaymentDetailsByRef(
            List<String> paymentRefs) {
        
        String placeholders = String.join(",", 
            Collections.nCopies(paymentRefs.size(), "?"));
        
        List<PaymentDetail> details = readJdbcTemplate.query(
            "SELECT payment_ref, amount, currency, customer_id, " +
            "       merchant_id, payment_method, description " +
            "FROM table2 " +
            "WHERE payment_ref IN (" + placeholders + ")",
            paymentRefs.toArray(),
            (rs, rowNum) -> new PaymentDetail(
                rs.getString("payment_ref"),
                rs.getBigDecimal("amount"),
                rs.getString("currency"),
                rs.getString("customer_id"),
                rs.getString("merchant_id"),
                rs.getString("payment_method"),
                rs.getString("description")
            )
        );
        
        return details.stream()
            .collect(Collectors.toMap(
                PaymentDetail::getPaymentRef, 
                p -> p
            ));
    }
    
    private void enrichPayments(
            Map<String, Long> refToIdMap, 
            Map<String, PaymentDetail> paymentDetails) {
        
        for (Map.Entry<String, Long> entry : refToIdMap.entrySet()) {
            String paymentRef = entry.getKey();
            Long paymentId = entry.getValue();
            PaymentDetail detail = paymentDetails.get(paymentRef);
            
            writeJdbcTemplate.update(
                "UPDATE table1 SET " +
                "amount = ?, currency = ?, customer_id = ?, " +
                "merchant_id = ?, payment_method = ?, " +
                "status = ?, completed_at = ? " +
                "WHERE payment_id = ?",
                detail.getAmount(),
                detail.getCurrency(),
                detail.getCustomerId(),
                detail.getMerchantId(),
                detail.getPaymentMethod(),
                "COMPLETED",
                new Timestamp(System.currentTimeMillis()),
                paymentId
            );
        }
    }
}

java// Enable retry in configuration
@Configuration
@EnableRetry
public class RetryConfig {
}    
