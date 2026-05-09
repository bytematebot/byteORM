package dev.bytemate.byteorm.intellij

import com.intellij.codeInsight.completion.CompletionContributor
import com.intellij.codeInsight.completion.CompletionParameters
import com.intellij.codeInsight.completion.CompletionProvider
import com.intellij.codeInsight.completion.CompletionResultSet
import com.intellij.codeInsight.completion.CompletionType
import com.intellij.codeInsight.lookup.LookupElementBuilder
import com.intellij.openapi.project.DumbAware
import com.intellij.patterns.PlatformPatterns
import com.intellij.psi.PsiFile
import com.intellij.util.ProcessingContext

class ByteormCompletionContributor : CompletionContributor(), DumbAware {
    init {
        extend(
            CompletionType.BASIC,
            PlatformPatterns.psiElement().withLanguage(ByteormLanguage),
            object : CompletionProvider<CompletionParameters>() {
                override fun addCompletions(
                    parameters: CompletionParameters,
                    context: ProcessingContext,
                    resultSet: CompletionResultSet,
                ) {
                    val file = parameters.originalFile
                    if (!file.isByteormFile()) {
                        return
                    }

                    val text = file.text
                    val offset = parameters.offset.coerceIn(0, text.length)
                    val prefix = currentPrefix(text, offset)
                    val lineBeforeCaret = currentLinePrefix(text, offset)
                    val symbols = ByteormSchemaSymbols.collectFromText(text)
                    val prefixedResultSet = resultSet.withPrefixMatcher(prefix)

                    val candidates = candidatesFor(text, offset, lineBeforeCaret, symbols)
                    for (candidate in candidates) {
                        if (prefix.isNotEmpty() && !candidate.startsWith(prefix, ignoreCase = true)) {
                            continue
                        }
                        prefixedResultSet.addElement(LookupElementBuilder.create(candidate))
                    }
                }
            },
        )
    }

    private fun PsiFile.isByteormFile(): Boolean {
        return language.`is`(ByteormLanguage) || fileType == ByteormFileType
    }

    private fun currentPrefix(text: String, offset: Int): String {
        if (offset <= 0) return ""
        var index = offset - 1
        while (index >= 0) {
            val ch = text[index]
            if (ch.isLetterOrDigit() || ch == '_' || ch == '?') {
                index--
            } else {
                break
            }
        }
        return text.substring(index + 1, offset)
    }

    private fun currentLinePrefix(text: String, offset: Int): String {
        val lineStart = text.lastIndexOf('\n', (offset - 1).coerceAtLeast(0))
            .let { if (it < 0) 0 else it + 1 }
        return text.substring(lineStart, offset)
    }

    private fun candidatesFor(
        text: String,
        offset: Int,
        lineBeforeCaret: String,
        symbols: ByteormSymbols,
    ): List<String> {
        val line = lineBeforeCaret.trimStart()
        val lower = line.lowercase()
        val block = currentBlock(text, offset)
        val tokens = line.trim().split(Regex("\\s+")).filter { it.isNotEmpty() }
        val tokenCount = tokens.size
        val endsWithWhitespace = lineBeforeCaret.lastOrNull()?.isWhitespace() == true
        val inDeclarationName = isDeclarationNamePosition(line)

        val types = builtinTypes()
        val entityNames = (symbols.models + symbols.enums).distinct()
        val enumOnlyNames = symbols.enumValues.distinct()
        val fieldNames = symbols.fieldsByModel.values.flatten().distinct()
        val attributes = listOf("default", "computed", "audit", "PrimaryKey", "NotNull", "Nullable", "Unique", "Index", "ForeignKey")
        val referentialActions = listOf("cascade", "restrict", "set null", "no action")

        return when {
            inDeclarationName -> emptyList()
            lower.contains("default(") -> fallback(
                listOf("now()", "true", "false", "null", "auto_increment"),
                enumOnlyNames,
                entityNames,
                types,
            )
            lower.contains("foreignkey(") -> fallback(
                entityNames,
                fieldNames,
                referentialActions,
            )
            lower.contains("ondelete") -> referentialActions
            block == Block.MODEL -> modelCandidates(tokens, endsWithWhitespace, types, entityNames, attributes)
            block == Block.ENUM -> emptyList()
            tokenCount <= 1 -> listOf("model", "enum")
            else -> emptyList()
        }.distinct()
    }

    private fun isDeclarationNamePosition(line: String): Boolean {
        return line.trimStart().matches(Regex("""(?:model|enum)\s+[A-Za-z_][A-Za-z0-9_]*\s*\{?\s*""")) ||
            line.trimStart().matches(Regex("""(?:model|enum)\s+"""))
    }

    private fun modelCandidates(
        tokens: List<String>,
        endsWithWhitespace: Boolean,
        types: List<String>,
        entityNames: List<String>,
        attributes: List<String>,
    ): List<String> {
        if (tokens.isEmpty()) {
            return emptyList()
        }

        val lastToken = tokens.last()
        return when {
            lastToken.startsWith("@") -> attributes
            tokens.size == 1 && endsWithWhitespace -> fallback(types, entityNames)
            tokens.size == 2 && !endsWithWhitespace -> fallback(types, entityNames)
            tokens.size >= 2 -> attributes
            else -> emptyList()
        }
    }

    private fun fallback(vararg groups: List<String>): List<String> {
        val result = linkedSetOf<String>()
        for (group in groups) {
            result.addAll(group)
        }
        return result.toList()
    }

    private fun builtinTypes(): List<String> = listOf(
        "String",
        "Text",
        "Int",
        "BigInt",
        "Float",
        "Real",
        "Boolean",
        "Date",
        "TimestamptZ",
        "JsonB",
        "Jsonb",
        "Serial",
    )

    private enum class Block {
        MODEL,
        ENUM,
        NONE,
    }

    private fun currentBlock(text: String, offset: Int): Block {
        var block = Block.NONE
        var depth = 0
        val prefix = text.substring(0, offset)
        for (line in prefix.lineSequence()) {
            val trimmed = line.trimStart()
            if (block == Block.NONE) {
                when {
                    trimmed.startsWith("model ") && trimmed.contains("{") -> {
                        block = Block.MODEL
                        depth = 1
                    }
                    trimmed.startsWith("enum ") && trimmed.contains("{") -> {
                        block = Block.ENUM
                        depth = 1
                    }
                }
            } else {
                depth += line.count { it == '{' }
                depth -= line.count { it == '}' }
                if (depth <= 0) {
                    block = Block.NONE
                    depth = 0
                }
            }
        }
        return block
    }
}
