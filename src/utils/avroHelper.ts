// @ts-ignore
import avro from 'avro-js';

/**
 * Custom Long Avro type used for deserializing large numbers with BitInt.
 * This fixes a deserialization bug with Avro not supporting large values.
 * @private
 */
console.log('avro.types.LongType', avro.types.LongType)
export const CustomLongAvroType = avro.types.LongType.using({
    fromBuffer: (buf:Buffer) => {
        const big = buf.readBigInt64LE();
        if (big < Number.MIN_SAFE_INTEGER || big > Number.MAX_SAFE_INTEGER) {
            return big;
        }
        return Number(BigInt.asIntN(64, big));
    },
    
    toBuffer: (n:number | bigint) => {
        const buf = Buffer.allocUnsafe(8)
        if (typeof n === 'bigint') {
        buf.writeBigInt64LE(n)
        } else {
        buf.writeBigInt64LE(BigInt(n))
        }
        return buf
    },
    fromJSON: (val: string) => BigInt(val),
    toJSON: (val: bigint) => Number(val),
    isValid: (n:unknown) => {
        return (typeof n === 'number' && n % 1 === 0) || typeof n === 'bigint'
    },
    compare: (n1: number | bigint, n2: number | bigint) => {
        return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
    }
});
