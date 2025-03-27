
version: 2



models:
  - name: online_trsn_over_time
    description: "For all transactions that happened online, to track the trend over time"
    columns:
      - name: transaction_year
        data_type: timestamp
        description: ""

      - name: online_transaction_amount
        data_type: numeric
        description: ""

      - name: total_transaction_amount
        data_type: numeric
        description: ""

      - name: cnt_of_transactions
        data_type: int64
        description: ""

      - name: cnt_of_online_transaction
        data_type: int64
        description: ""

  - name: clients_consumption_2019
    description: ""
    columns:
  - name: transaction_fact
    description: "It combines tables user, cards and transactions together"
    columns:
      - name: transaction_date
        data_type: date
        description: ""

      - name: transaction_amount
        data_type: numeric
        description: ""

      - name: merchant_country
        data_type: string
        description: ""

      - name: merchant_state
        data_type: string
        description: ""

      - name: merchant_city
        data_type: string
        description: ""

      - name: is_online_transaction
        data_type: boolean
        description: ""

      - name: transaction_status
        data_type: string
        description: ""

      - name: client_id
        data_type: int64
        description: ""

      - name: card_id
        data_type: int64
        description: ""

      - name: gender
        data_type: string
        description: ""

      - name: if_above_avg_income
        data_type: boolean
        description: ""

      - name: state
        data_type: string
        description: ""

      - name: city
        data_type: string
        description: ""

      - name: country
        data_type: string
        description: ""

      - name: yearly_income
        data_type: numeric
        description: ""

      - name: total_debt
        data_type: numeric
        description: ""

      - name: credit_score
        data_type: numeric
        description: ""

      - name: card_type
        data_type: string
        description: ""

      - name: card_expired_date
        data_type: date
        description: ""

      - name: acct_open_date
        data_type: date
        description: ""

      - name: credit_limit
        data_type: numeric
        description: ""
