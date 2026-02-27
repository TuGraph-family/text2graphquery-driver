-- Schema for FInancial_Payment

CREATE TABLE `PaymentTransaction` (
  `transaction_id`     STRING(MAX) NOT NULL,
  `amount`             FLOAT64,
  `currency`           STRING(MAX),
  `status`             STRING(MAX),
  `created_at`         STRING(MAX),
  `completed_at`       STRING(MAX),
  `reference_number`   STRING(MAX)
) PRIMARY KEY (`transaction_id`);

CREATE TABLE `Account` (
  `account_id`         STRING(MAX) NOT NULL,
  `account_type`       STRING(MAX),
  `balance`            FLOAT64,
  `currency`           STRING(MAX),
  `status`             STRING(MAX),
  `opened_date`        DATE,
  `last_activity`      STRING(MAX)
) PRIMARY KEY (`account_id`);

CREATE TABLE `Customer` (
  `customer_id`        STRING(MAX) NOT NULL,
  `customer_type`      STRING(MAX),
  `name`               STRING(MAX),
  `email`              STRING(MAX),
  `phone`              STRING(MAX),
  `address`            STRING(MAX),
  `kyc_status`         STRING(MAX)
) PRIMARY KEY (`customer_id`);

CREATE TABLE `PaymentMethod` (
  `method_id`          STRING(MAX) NOT NULL,
  `method_type`        STRING(MAX),
  `provider`           STRING(MAX),
  `status`             STRING(MAX),
  `expiry_date`        DATE,
  `masked_identifier`  STRING(MAX)
) PRIMARY KEY (`method_id`);

CREATE TABLE `Merchant` (
  `merchant_id`        STRING(MAX) NOT NULL,
  `business_name`      STRING(MAX),
  `category_code`      STRING(MAX),
  `status`             STRING(MAX),
  `contract_terms`     STRING(MAX)
) PRIMARY KEY (`merchant_id`);

CREATE TABLE `ComplianceRule` (
  `rule_id`            STRING(MAX) NOT NULL,
  `jurisdiction`       STRING(MAX),
  `rule_type`          STRING(MAX),
  `effective_date`     DATE,
  `description`        STRING(MAX),
  `monitoring_required` BOOL
) PRIMARY KEY (`rule_id`);

CREATE TABLE `AuditLog` (
  `log_id`             STRING(MAX) NOT NULL,
  `event_type`         STRING(MAX),
  `timestamp`          STRING(MAX),
  `details`            STRING(MAX)
) PRIMARY KEY (`log_id`);

CREATE TABLE `RiskAssessment` (
  `assessment_id`      STRING(MAX) NOT NULL,
  `score`              INT64,
  `assessed_at`        STRING(MAX),
  `risk_level`         STRING(MAX)
) PRIMARY KEY (`assessment_id`);

CREATE TABLE `CustomerInitiatesPaymentTransaction` (
  `SRC_ID`           STRING(MAX) NOT NULL,
  `DST_ID`           STRING(MAX) NOT NULL,
  FOREIGN KEY (`SRC_ID`) REFERENCES `Customer` (`customer_id`),
  FOREIGN KEY (`DST_ID`) REFERENCES `PaymentTransaction` (`transaction_id`)
) PRIMARY KEY (`SRC_ID`, `DST_ID`);

CREATE TABLE `PaymentTransactionFundsFromAccount` (
  `SRC_ID`           STRING(MAX) NOT NULL,
  `DST_ID`           STRING(MAX) NOT NULL,
  FOREIGN KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`),
  FOREIGN KEY (`DST_ID`) REFERENCES `Account` (`account_id`)
) PRIMARY KEY (`SRC_ID`, `DST_ID`);

CREATE TABLE `PaymentTransactionReceivesToAccount` (
  `SRC_ID`           STRING(MAX) NOT NULL,
  `DST_ID`           STRING(MAX) NOT NULL,
  FOREIGN KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`),
  FOREIGN KEY (`DST_ID`) REFERENCES `Account` (`account_id`)
) PRIMARY KEY (`SRC_ID`, `DST_ID`);

CREATE TABLE `PaymentTransactionAuthorizedByPaymentMethod` (
  `SRC_ID`           STRING(MAX) NOT NULL,
  `DST_ID`           STRING(MAX) NOT NULL,
  FOREIGN KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`),
  FOREIGN KEY (`DST_ID`) REFERENCES `PaymentMethod` (`method_id`)
) PRIMARY KEY (`SRC_ID`, `DST_ID`);

CREATE TABLE `PaymentTransactionProcessedForMerchant` (
  `SRC_ID`           STRING(MAX) NOT NULL,
  `DST_ID`           STRING(MAX) NOT NULL,
  FOREIGN KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`),
  FOREIGN KEY (`DST_ID`) REFERENCES `Merchant` (`merchant_id`)
) PRIMARY KEY (`SRC_ID`, `DST_ID`);

CREATE TABLE `PaymentTransactionGovernedByComplianceRule` (
  `SRC_ID`           STRING(MAX) NOT NULL,
  `DST_ID`           STRING(MAX) NOT NULL,
  FOREIGN KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`),
  FOREIGN KEY (`DST_ID`) REFERENCES `ComplianceRule` (`rule_id`)
) PRIMARY KEY (`SRC_ID`, `DST_ID`);

CREATE TABLE `PaymentTransactionHasAuditLogAuditLog` (
  `SRC_ID`           STRING(MAX) NOT NULL,
  `DST_ID`           STRING(MAX) NOT NULL,
  FOREIGN KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`),
  FOREIGN KEY (`DST_ID`) REFERENCES `AuditLog` (`log_id`)
) PRIMARY KEY (`SRC_ID`, `DST_ID`);

CREATE TABLE `PaymentTransactionHasRiskAssessmentRiskAssessment` (
  `SRC_ID`           STRING(MAX) NOT NULL,
  `DST_ID`           STRING(MAX) NOT NULL,
  FOREIGN KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`),
  FOREIGN KEY (`DST_ID`) REFERENCES `RiskAssessment` (`assessment_id`)
) PRIMARY KEY (`SRC_ID`, `DST_ID`);

CREATE OR REPLACE PROPERTY GRAPH `FInancial_Payment`
  NODE TABLES (`PaymentTransaction`, `Account`, `Customer`, `PaymentMethod`, `Merchant`, `ComplianceRule`, `AuditLog`, `RiskAssessment`)
  EDGE TABLES (
    `CustomerInitiatesPaymentTransaction`
      SOURCE KEY (`SRC_ID`) REFERENCES `Customer` (`customer_id`)
      DESTINATION KEY (`DST_ID`) REFERENCES `PaymentTransaction` (`transaction_id`)
      LABEL `Initiates`,
    `PaymentTransactionFundsFromAccount`
      SOURCE KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`)
      DESTINATION KEY (`DST_ID`) REFERENCES `Account` (`account_id`)
      LABEL `FundsFrom`,
    `PaymentTransactionReceivesToAccount`
      SOURCE KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`)
      DESTINATION KEY (`DST_ID`) REFERENCES `Account` (`account_id`)
      LABEL `ReceivesTo`,
    `PaymentTransactionAuthorizedByPaymentMethod`
      SOURCE KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`)
      DESTINATION KEY (`DST_ID`) REFERENCES `PaymentMethod` (`method_id`)
      LABEL `AuthorizedBy`,
    `PaymentTransactionProcessedForMerchant`
      SOURCE KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`)
      DESTINATION KEY (`DST_ID`) REFERENCES `Merchant` (`merchant_id`)
      LABEL `ProcessedFor`,
    `PaymentTransactionGovernedByComplianceRule`
      SOURCE KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`)
      DESTINATION KEY (`DST_ID`) REFERENCES `ComplianceRule` (`rule_id`)
      LABEL `GovernedBy`,
    `PaymentTransactionHasAuditLogAuditLog`
      SOURCE KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`)
      DESTINATION KEY (`DST_ID`) REFERENCES `AuditLog` (`log_id`)
      LABEL `HasAuditLog`,
    `PaymentTransactionHasRiskAssessmentRiskAssessment`
      SOURCE KEY (`SRC_ID`) REFERENCES `PaymentTransaction` (`transaction_id`)
      DESTINATION KEY (`DST_ID`) REFERENCES `RiskAssessment` (`assessment_id`)
      LABEL `HasRiskAssessment`
  );