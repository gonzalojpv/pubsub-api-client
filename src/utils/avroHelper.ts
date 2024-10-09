// @ts-ignore
import avro from 'avro-js';

/**
 * Custom Long Avro type used for deserializing large numbers with BitInt.
 * This fixes a deserialization bug with Avro not supporting large values.
 * @private
 */
export const CustomLongAvroType = avro.types.LongType.using({
    // @ts-ignore
    fromBuffer: (buf) => {
        const big = buf.readBigInt64LE();
        if (big < Number.MIN_SAFE_INTEGER || big > Number.MAX_SAFE_INTEGER) {
            return big;
        }
        return Number(BigInt.asIntN(64, big));
    },
    // @ts-ignore
    toBuffer: (n) => {
        const buf = Buffer.allocUnsafe(8);
        if (n instanceof BigInt) {
            // @ts-ignore
            buf.writeBigInt64LE(n);
        } else {
            buf.writeBigInt64LE(BigInt(n));
        }
        return buf;
    },
    fromJSON: BigInt,
    toJSON: Number,
    // @ts-ignore
    isValid: (n) => {
        const type = typeof n;
        return (type === 'number' && n % 1 === 0) || type === 'bigint';
    },
    // @ts-ignore
    compare: (n1, n2) => {
        return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
    }
});
