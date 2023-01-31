const { getJsonFromCsv } = require("convert-csv-to-json")
const jsonToCsv = require("json-to-csv")
const { isEmpty, find, flattenDepth } = require("lodash")
const { parse, differenceInMilliseconds } = require("date-fns")
const COMMAND_LINE_ARGS = require("minimist")(process.argv.slice(2))
const CSV_INPUT_FILE = COMMAND_LINE_ARGS.new_input
const CSV_OUTPUT_FILE =
    COMMAND_LINE_ARGS.output ||
    "./OUTPUT_malignant_neoplasms_digestive_admission_merged.csv"

// 2119-03-08 08:00:00
const parseDate = (date) => parse(date, "yyyy-MM-dd HH:mm:ss", new Date())

const main = async () => {
    const newCompressedMap = await iterateOverFile(CSV_INPUT_FILE)

    convertJsonToCsv(newCompressedMap, CSV_OUTPUT_FILE)
}

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
                                    ) < 3600000) ||
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

    return { ...entry, "concept:name": conceptName }
}

main()
