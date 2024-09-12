package com.lbry.database;

public enum Prefix{
    CLAIM_TO_SUPPORT('K'),
    SUPPORT_TO_CLAIM('L'),

    CLAIM_TO_TXO('E'),
    TXO_TO_CLAIM('G'),

    CLAIM_TO_CHANNEL('I'),
    CHANNEL_TO_CLAIM('J'),

    CLAIM_SHORT_ID_PREFIX('F'),
    BID_ORDER('D'),
    CLAIM_EXPIRATION('O'),

    CLAIM_TAKEOVER('P'),
    PENDING_ACTIVATION('Q'),
    ACTIVATED_CLAIM_AND_SUPPORT('R'),
    ACTIVE_AMOUNT('S'),

    REPOST('V'),
    REPOSTED_CLAIM('W'),

    UNDO('M'),
    TOUCHED_OR_DELETED('Y'),

    TX('B'),
    BLOCK_HASH('C'),
    HEADER('H'),
    TX_NUM('N'),
    TX_COUNT('T'),
    TX_HASH('X'),
    UTXO('u'),
    HASHX_UTXO('h'),
    HASHX_HISTORY('x'),
    DB_STATE('s'),
    CHANNEL_COUNT('Z'),
    SUPPORT_AMOUNT('a'),
    BLOCK_TX('b'),
    TRENDING_NOTIFICATION('c'),
    MEMPOOL_TX('d'),
    TOUCHED_HASHX('e'),
    HASHX_STATUS('f'),
    HASHX_MEMPOOL_STATUS('g'),
    REPOSTED_COUNT('j'),
    EFFECTIVE_AMOUNT('i'),
    FUTURE_EFFECTIVE_AMOUNT('k'),
    HASHX_HISTORY_HASH('l');

    private final byte value;

    Prefix(char value){
        this((byte) value);
    }

    Prefix(byte value){
        this.value = value;
    }

    public byte getValue(){
        return this.value;
    }

    public static Prefix getByValue(char value){
        return Prefix.getByValue((byte) value);
    }

    public static Prefix getByValue(byte value){
        for(Prefix p : Prefix.values()){
            if(p.value==value){
                return p;
            }
        }
        return null;
    }

}