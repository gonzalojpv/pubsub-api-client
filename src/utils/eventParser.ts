// @ts-nocheck
import avro from "avro-js";
import { z } from 'zod'
import { ActionLogger } from '@prismatic-io/spectral'
import { ConsumerEvent } from '../pubsub_api'
import EventParseError from './eventParseError'


export interface AvroSchema {
  type: {
    getFields: () => AvroField[]
    fromBuffer: (buffer: Buffer) => unknown
  }
}

interface AvroField {
  _name: string
  _type: {
    getTypes: () => AvroField[]
  }
  getName: () => string
  getFields: () => AvroField[]
}

// Define the Zod schema for `ParsedPayload`
const ParsedPayloadSchema = z.object({
  ChangeEventHeader: z
    .object({
      // are we sure that all these fields aren't optional?
      // if they are optional, and any of them are missing, the parse will fail and parseEvent wont do anything
      nulledFields: z.array(z.string()),
      diffFields: z.array(z.string()),
      changedFields: z.array(z.string()),
    })
    .optional(),
})
type ParsedPayload = z.infer<typeof ParsedPayloadSchema>

function isParsedPayload(payload: unknown): payload is ParsedPayload {
  return ParsedPayloadSchema.safeParse(payload).success
}

/**
 * Parses the Avro encoded data of an event agains a schema
 * @param {*} schema Avro schema
 * @param {*} event Avro encoded data of the event
 * @returns {*} parsed event data
 * @protected
 */
// @ts-ignore
export function parseEvent(schema: AvroSchema, event: ConsumerEvent, logger) {
   if (!event.event) {
    const error = new Error('Event data is missing in the response')
    throw new EventParseError('Failed to process the event due to missing event data', error)
  }

  const allFields = schema.type.getFields()
  const replayId = decodeReplayId(event.replayId)
  logger.info('Decoded replayId', replayId)
  logger.info('_read', '_read' in schema.type)
  const payload = schema.type.fromBuffer(event.event.payload)
  logger.info('payload replayId', payload)
  // Parse CDC header if available
  if (payload.ChangeEventHeader) {
    const changeEventHeader = payload.ChangeEventHeader
    try {
      changeEventHeader.nulledFields = parseFieldBitmaps(allFields, changeEventHeader.nulledFields)
    } catch (error) {
      throw new EventParseError('Failed to parse nulledFields', error as Error)
    }
    try {
      changeEventHeader.diffFields = parseFieldBitmaps(allFields, changeEventHeader.diffFields)
    } catch (error) {
      throw new EventParseError('Failed to parse nulledFields', error as Error)
    }
    try {
      changeEventHeader.changedFields = parseFieldBitmaps(
        allFields,
        changeEventHeader.changedFields
      )
    } catch (error) {
      throw new EventParseError('Failed to parse changedFields', error as Error)
    }
  }
  // Eliminate intermediate types left by Avro in payload
  flattenSinglePropertyObjects(payload);
  // Return parsed data
  return {
    replayId,
    payload,
  };
}

interface NestedObject {
  [key: string]: NestedObject | string | number | boolean | null // Adjust this as needed
}

function flattenSinglePropertyObjects(theObject: NestedObject) {
  Object.entries(theObject).forEach(([key, value]) => {
    // Check if the key is not 'ChangeEventHeader' and the value is an object
    if (key !== 'ChangeEventHeader' && value && typeof value === 'object') {
      const subKeys = Object.keys(value)
      if (subKeys.length === 1) {
        const subValue = value[subKeys[0]] as NestedObject // Type assertion
        theObject[key] = subValue // Replace the original value with the subValue

        // Recursively flatten if the subValue is also an object
        if (subValue && typeof subValue === 'object') {
          flattenSinglePropertyObjects(theObject[key]) // Type assertion
        }
      }
    }
  })
}

// Update parseFieldBitmaps function
function parseFieldBitmaps(allFields: AvroField[], fieldBitmapsAsHex: string[]) {
  if (fieldBitmapsAsHex.length === 0) {
    return []
  }

  let fieldNames: string[] = []
  // Replace top field level bitmap with list of fields
  if (fieldBitmapsAsHex[0].startsWith('0x')) {
    fieldNames = getFieldNamesFromBitmap(allFields, fieldBitmapsAsHex[0])
  }

  // Process compound fields
  if (
    fieldBitmapsAsHex.length > 1 &&
    fieldBitmapsAsHex[fieldBitmapsAsHex.length - 1].includes('-')
  ) {
    fieldBitmapsAsHex.forEach(fieldBitmapAsHex => {
      const bitmapMapStrings = fieldBitmapAsHex.split('-')
      // Ignore top-level field bitmap
      if (bitmapMapStrings.length >= 2) {
        const parentField = allFields[parseInt(bitmapMapStrings[0], 10)]
        const childFields = getChildFields(parentField)
        const childFieldNames = getFieldNamesFromBitmap(childFields, bitmapMapStrings[1])
        fieldNames = fieldNames.concat(
          childFieldNames.map(fieldName => `${parentField._name}.${fieldName}`)
        )
      }
    })
  }
  return fieldNames
}

// Update getChildFields function
function getChildFields(parentField: AvroField): AvroField[] {
  const types = parentField._type.getTypes()
  let fields: AvroField[] = []
  types.forEach(type => {
    if (type instanceof avro.types.RecordType) {
      fields = fields.concat(type.getFields())
    }
  })
  return fields
}

// Update getFieldNamesFromBitmap function
function getFieldNamesFromBitmap(fields: AvroField[], fieldBitmapAsHex: string) {
  // Convert hex to binary and reverse bits
  let binValue = hexToBin(fieldBitmapAsHex)
  binValue = binValue.split('').reverse().join('')

  // Use bitmap to figure out field names based on index
  const fieldNames: string[] = []
  for (let i = 0; i < binValue.length && i < fields.length; i++) {
    if (binValue[i] === '1') {
      fieldNames.push(fields[i].getName())
    }
  }
  return fieldNames
}

// Update decodeReplayId function
export function decodeReplayId(encodedReplayId: Buffer): number {
  return Number(encodedReplayId.readBigUInt64BE())
}

// Update encodeReplayId function
export function encodeReplayId(replayId: number): Buffer {
  const buf = Buffer.allocUnsafe(8)
  buf.writeBigUInt64BE(BigInt(replayId), 0)
  return buf
}

/**
 * Converts a hexadecimal string into a string binary representation
 * @param {string} hex
 * @returns {string}
 * @private
 */
// @ts-ignore
function hexToBin(hex: string): string {
  let bin = hex.substring(2) // Remove 0x prefix
  bin = bin
    .replace(/0/g, '0000')
    .replace(/1/g, '0001')
    .replace(/2/g, '0010')
    .replace(/3/g, '0011')
    .replace(/4/g, '0100')
    .replace(/5/g, '0101')
    .replace(/6/g, '0110')
    .replace(/7/g, '0111')
    .replace(/8/g, '1000')
    .replace(/9/g, '1001')
    .replace(/A/g, '1010')
    .replace(/B/g, '1011')
    .replace(/C/g, '1100')
    .replace(/D/g, '1101')
    .replace(/E/g, '1110')
    .replace(/F/g, '1111')
  return bin
}
