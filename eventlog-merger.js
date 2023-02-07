const { getJsonFromCsv } = require("convert-csv-to-json")
const jsonToCsv = require("json-to-csv")
const { isEmpty, find, flattenDepth } = require("lodash")
const { parse, differenceInMilliseconds } = require("date-fns")
const COMMAND_LINE_ARGS = require("minimist")(process.argv.slice(2))
const CSV_INPUT_FILE = COMMAND_LINE_ARGS.input_file
const DUPLICATE_REMOVAL_TIME_FRAME = COMMAND_LINE_ARGS.time_frame || 3600000 // 1h
const CSV_OUTPUT_FILE =
    COMMAND_LINE_ARGS.output_file || "./merged_output_file.csv"

// 2119-03-08 08:00:00
const parseDate = (date) => parse(date, "yyyy-MM-dd HH:mm:ss", new Date())

const main = async () => {
    const newCompressedMap = await iterateOverFile(CSV_INPUT_FILE)

    convertJsonToCsv(newCompressedMap, CSV_OUTPUT_FILE)
}

const transferEvents = Object.keys({
    "transfer to Hematology/Oncology": 1763,
    "transfer to Medicine": 1344,
    "admit at Medicine": 1318,
    "transfer to Med/Surg": 1238,
    "transfer to Transplant": 1091,
    "transfer to Discharge Lounge": 10,
    "admit at Transplant": 834,
    "admit at Med/Surg": 494,
    "transfer to Surgical Intensive Care Unit (SICU)": 458,
    "transfer to Surgery/Pancreatic/Biliary/Bariatric": 445,
    "transfer to PACU": 436,
    "transfer to Medical/Surgical Intensive Care Unit (MICU/SICU)": 416,
    "admit at Med/Surg/GYN": 352,
    "transfer to Med/Surg/GYN": 299,
    "transfer to Trauma SICU (TSICU)": 298,
    "admit at Medical/Surgical Intensive Care Unit (MICU/SICU)": 268,
    "admit at Medical Intensive Care Unit (MICU)": 226,
    "transfer to Emergency Department Observation": 210,
    "transfer to Medical Intensive Care Unit (MICU)": 174,
    "transfer to Medical/Surgical (Gynecology)": 148,
    "admit at Surgery/Pancreatic/Biliary/Bariatric": 127,
    "transfer to Neurology": 119,
    "transfer to Med/Surg/Trauma": 112,
    "transfer to Vascular": 111,
    "admit at PACU": 109,
    "admit at Observation": 105,
    "admit at Surgical Intensive Care Unit (SICU)": 102,
    "transfer to Surgery/Trauma": 99,
    "admit at Vascular": 92,
    "admit at Medical/Surgical (Gynecology)": 91,
    "admit at Emergency Department Observation": 90,
    "admit at Med/Surg/Trauma": 82,
    "transfer to Medicine/Cardiology": 80,
    "admit at Neurology": 76,
    "admit at Medicine/Cardiology": 76,
    "transfer to Thoracic Surgery": 55,
    "admit at Trauma SICU (TSICU)": 53,
    "transfer to Coronary Care Unit (CCU)": 47,
    "admit at Surgery/Trauma": 43,
    "admit at Coronary Care Unit (CCU)": 32,
    "transfer to Cardiac Vascular Intensive Care Unit (CVICU)": 30,
    "admit at Thoracic Surgery": 26,
    "transfer to Cardiac Surgery": 24,
    "transfer to Observation": 20,
    "admit at Cardiac Surgery": 18,
    "transfer to Psychiatry": 11,
    "admit at Psychiatry": 9,
    "admit at Cardiac Vascular Intensive Care Unit (CVICU)": 8,
    "admit at Obstetrics (Postpartum & Antepartum)": 1,
    "transfer to Labor & Delivery": 1,
    "transfer to Obstetrics (Postpartum & Antepartum)": 1,
})

const convertJsonToCsv = async (compressedJsonArray, outputFile) => {
    jsonToCsv(compressedJsonArray, outputFile)
        .then(() => {
            console.log("200")
        })
        .catch((error) => {
            console.log("error:", error)
        })
}

const isDead = (a, b) =>
    (a === "death" && b === "discharge to DIED") ||
    (a === "discharge to DIED" && b === "death") ||
    (a === "discharge to DIED" && b === "discharge to DIED") ||
    (a === "death" && b === "death")

const iterateOverFile = async (inputFile) => {
    const compressedJson = await getJsonFromCsv(inputFile)
    let eventStore = []
    compressedJson
        .map((entry) => parseEntry(entry))
        .filter((entry) => {
            if (
                entry["concept:name"] === "edreg" ||
                entry["concept:name"] === "edout"
            ) {
                return false
            }

            return true
        })
        .sort((current, previous) => {
            if (current["subject_id"] === previous["subject_id"]) {
                {
                    const previousTimestamp = parseDate(
                        previous["time:timestamp"]
                    )
                    const currentTimestamp = parseDate(
                        current["time:timestamp"]
                    )
                    return differenceInMilliseconds(
                        currentTimestamp,
                        previousTimestamp
                    )
                }
            } else {
                return current["subject_id"] - previous["subject_id"]
            }
        })
        .reduce(
            (newMap, elem) =>
                newMap.has(elem["subject_id"])
                    ? newMap.set(elem["subject_id"], [
                          ...newMap.get(elem["subject_id"]),
                          elem,
                      ])
                    : newMap.set(elem["subject_id"], [elem]),
            new Map()
        )
        .forEach((value) => {
            const filteredArrayBySubjectId = value.reduce(
                (acc, current) =>
                    !isEmpty(
                        find(
                            acc,
                            (entry) =>
                                (entry["concept:name"] ===
                                    current["concept:name"] &&
                                    Math.abs(
                                        differenceInMilliseconds(
                                            parseDate(entry["time:timestamp"]),
                                            parseDate(current["time:timestamp"])
                                        )
                                    ) < DUPLICATE_REMOVAL_TIME_FRAME) ||
                                isDead(
                                    entry["concept:name"],
                                    current["concept:name"]
                                )
                        )
                    )
                        ? acc
                        : [
                              ...acc,
                              current["concept:name"] === "death"
                                  ? {
                                        ...current,
                                        "concept:name": "discharge to DIED",
                                    }
                                  : current,
                          ],

                []
            )
            eventStore = [...eventStore, filteredArrayBySubjectId]
        })
    return flattenDepth(eventStore, 1)
}

const parseEntry = (entry) => {
    let conceptName = entry["concept:name"]
    // transfer
    if (conceptName === "transfer") {
        conceptName += ` to ${entry["eventtype"]}`
    }

    // admission
    if (conceptName === "admit" && !isEmpty(entry["eventtype"])) {
        conceptName += ` at ${entry["eventtype"]}`
    } else if (
        conceptName === "admit" &&
        isEmpty(entry["eventtype"]) &&
        !isEmpty(entry["case:admission_location"])
    ) {
        conceptName += ` at ${entry["case:admission_location"]}`
    }

    // discharge
    if (
        conceptName === "discharge" &&
        !isEmpty(entry["case:discharge_location"])
    ) {
        conceptName += ` to ${entry["case:discharge_location"]}`
    }

    if (transferEvents.includes(conceptName)) {
        conceptName = "TRANSFERS"
    }

    if (
        conceptName === "discharge to DIED" ||
        conceptName === "discharge to HOSPICE"
    ) {
        conceptName = "discharge to DIED_HOSPICE"
    }
    return { ...entry, "concept:name": conceptName }
}

main()
