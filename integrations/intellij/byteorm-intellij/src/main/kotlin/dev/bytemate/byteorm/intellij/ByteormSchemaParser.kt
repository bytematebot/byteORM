package dev.bytemate.byteorm.intellij

data class ParsedByteormSchema(
    val models: List<String>,
    val enums: List<String>,
    val enumValuesByEnum: Map<String, List<String>>,
    val fieldsByModel: Map<String, List<String>>,
)

object ByteormSchemaParser {
    private val modelPattern = Regex("""^\s*model\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{""")
    private val enumPattern = Regex("""^\s*enum\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{""")
    private val fieldPattern = Regex("""^\s*([A-Za-z_][A-Za-z0-9_]*)\s+[A-Za-z_][A-Za-z0-9_?]*\b""")
    private val enumValuePattern = Regex("""^\s*([A-Za-z_][A-Za-z0-9_]*)\b""")

    fun parse(text: String): ParsedByteormSchema {
        val models = linkedSetOf<String>()
        val enums = linkedSetOf<String>()
        val enumValuesByEnum = linkedMapOf<String, LinkedHashSet<String>>()
        val fieldsByModel = linkedMapOf<String, LinkedHashSet<String>>()

        var currentModel: String? = null
        var currentEnum: String? = null
        var braceDepth = 0

        for (line in text.lineSequence()) {
            val trimmed = line.trim()

            if (currentModel == null && currentEnum == null) {
                enumPattern.find(trimmed)?.let { match ->
                    currentEnum = match.groupValues[1]
                    enums.add(currentEnum!!)
                    enumValuesByEnum.getOrPut(currentEnum!!) { linkedSetOf() }
                    braceDepth = 1
                    continue
                }

                modelPattern.find(trimmed)?.let { match ->
                    currentModel = match.groupValues[1]
                    models.add(currentModel!!)
                    braceDepth = 1
                    continue
                }

                continue
            }

            braceDepth += line.count { it == '{' }
            braceDepth -= line.count { it == '}' }

            if (braceDepth <= 0) {
                currentModel = null
                currentEnum = null
                braceDepth = 0
                continue
            }

            if (trimmed.isEmpty() || trimmed.startsWith("#") || trimmed.startsWith("@")) {
                continue
            }

            if (currentEnum != null) {
                enumValuePattern.find(trimmed)?.let { match ->
                    val value = match.groupValues[1]
                    if (value != "enum") {
                        enumValuesByEnum.getOrPut(currentEnum!!) { linkedSetOf() }.add(value)
                    }
                }
                continue
            }

            if (currentModel != null) {
                fieldPattern.find(trimmed)?.let { match ->
                    fieldsByModel.getOrPut(currentModel!!) { linkedSetOf() }
                        .add(match.groupValues[1])
                }
            }
        }

        return ParsedByteormSchema(
            models = models.toList(),
            enums = enums.toList(),
            enumValuesByEnum = enumValuesByEnum.mapValues { it.value.toList() },
            fieldsByModel = fieldsByModel.mapValues { it.value.toList() },
        )
    }
}
