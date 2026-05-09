package dev.bytemate.byteorm.intellij

import com.intellij.lang.annotation.AnnotationHolder
import com.intellij.lang.annotation.Annotator
import com.intellij.lang.annotation.HighlightSeverity
import com.intellij.openapi.project.DumbAware
import com.intellij.openapi.util.TextRange
import com.intellij.psi.PsiElement

class ByteormAnnotator : Annotator, DumbAware {
    override fun annotate(element: PsiElement, holder: AnnotationHolder) {
        if (element !is ByteormFile) {
            return
        }

        val text = element.text
        val symbols = ByteormSchemaSymbols.collectFromText(text)
        val knownTypes = builtinTypes + symbols.models + symbols.enums
        validateLines(text, knownTypes.toSet(), holder)
    }

    private fun validateLines(text: String, knownTypes: Set<String>, holder: AnnotationHolder) {
        var block = Block.NONE
        var lineStart = 0

        while (lineStart <= text.length) {
            val lineEnd = text.indexOf('\n', lineStart).let { if (it < 0) text.length else it }
            val rawLine = text.substring(lineStart, lineEnd).trimEnd('\r')
            val contentEnd = lineStart + rawLine.length
            val commentStart = findCommentStart(rawLine)
            val line = rawLine.substring(0, commentStart).trim()

            if (line.isNotEmpty()) {
                block = validateLine(rawLine, lineStart, contentEnd, line, block, knownTypes, holder)
            }

            if (lineEnd == text.length) {
                break
            }
            lineStart = lineEnd + 1
        }
    }

    private fun validateLine(
        rawLine: String,
        lineStart: Int,
        contentEnd: Int,
        line: String,
        block: Block,
        knownTypes: Set<String>,
        holder: AnnotationHolder,
    ): Block {
        return when (block) {
            Block.NONE -> validateTopLevel(rawLine, lineStart, contentEnd, line, holder)
            Block.MODEL -> validateModelLine(rawLine, lineStart, contentEnd, line, knownTypes, holder)
            Block.ENUM -> validateEnumLine(rawLine, lineStart, contentEnd, line, holder)
        }
    }

    private fun validateTopLevel(
        rawLine: String,
        lineStart: Int,
        contentEnd: Int,
        line: String,
        holder: AnnotationHolder,
    ): Block {
        val keyword = line.substringBefore(' ')
        if (keyword != "model" && keyword != "enum") {
            error(holder, lineRange(rawLine, lineStart, contentEnd), "Expected 'model' or 'enum' declaration")
            return Block.NONE
        }

        val declaration = declarationPattern.matchEntire(line)
        if (declaration == null) {
            error(holder, lineRange(rawLine, lineStart, contentEnd), "Expected '$keyword Name {'")
            return Block.NONE
        }

        return if (keyword == "model") Block.MODEL else Block.ENUM
    }

    private fun validateModelLine(
        rawLine: String,
        lineStart: Int,
        contentEnd: Int,
        line: String,
        knownTypes: Set<String>,
        holder: AnnotationHolder,
    ): Block {
        if (line == "}") {
            return Block.NONE
        }

        if (line.contains('}')) {
            error(holder, lineRange(rawLine, lineStart, contentEnd), "Closing brace must be on its own line")
            return Block.MODEL
        }

        if (computedPattern.matches(line) || modelAttributePattern.matches(line)) {
            return Block.MODEL
        }

        val tokens = tokenize(rawLine, lineStart)
        if (tokens.size < 2) {
            error(holder, lineRange(rawLine, lineStart, contentEnd), "Expected field declaration: name Type")
            return Block.MODEL
        }

        val fieldName = tokens[0]
        val typeName = tokens[1]
        if (!identifierPattern.matches(fieldName.text)) {
            error(holder, fieldName.range, "Invalid field name")
        }

        val normalizedType = typeName.text.removeSuffix("?")
        if (!typeNamePattern.matches(typeName.text) || normalizedType !in knownTypes) {
            error(holder, typeName.range, "Unknown ByteORM type '${typeName.text}'")
        }

        for (token in tokens.drop(2)) {
            validateModifierOrAttribute(token, holder)
        }

        return Block.MODEL
    }

    private fun validateEnumLine(
        rawLine: String,
        lineStart: Int,
        contentEnd: Int,
        line: String,
        holder: AnnotationHolder,
    ): Block {
        if (line == "}") {
            return Block.NONE
        }

        if (!identifierPattern.matches(line)) {
            error(holder, lineRange(rawLine, lineStart, contentEnd), "Expected enum value name")
        }

        return Block.ENUM
    }

    private fun validateModifierOrAttribute(token: Token, holder: AnnotationHolder) {
        val text = token.text
        when {
            text.startsWith("@") -> {
                val name = text.removePrefix("@").substringBefore("(")
                if (name !in fieldAttributes) {
                    error(holder, token.range, "Unknown field attribute '$name'")
                }
            }
            else -> {
                val name = text.substringBefore("(")
                if (name !in modifiers) {
                    error(holder, token.range, "Unknown field modifier '$name'")
                }
            }
        }
    }

    private fun tokenize(rawLine: String, lineStart: Int): List<Token> {
        val tokens = mutableListOf<Token>()
        var index = 0

        while (index < rawLine.length) {
            while (index < rawLine.length && rawLine[index].isWhitespace()) {
                index++
            }
            if (index >= rawLine.length || rawLine[index] == '#') {
                break
            }

            val start = index
            var parenDepth = 0
            var inString: Char? = null
            while (index < rawLine.length) {
                val ch = rawLine[index]
                val quote = inString
                when {
                    quote != null -> {
                        if (ch == quote && (index == start || rawLine[index - 1] != '\\')) {
                            inString = null
                        }
                    }
                    ch == '"' || ch == '\'' -> inString = ch
                    ch == '(' -> parenDepth++
                    ch == ')' -> parenDepth = (parenDepth - 1).coerceAtLeast(0)
                    ch == '#' && parenDepth == 0 -> break
                    ch.isWhitespace() && parenDepth == 0 -> break
                }
                index++
            }

            tokens.add(Token(rawLine.substring(start, index), TextRange(lineStart + start, lineStart + index)))
        }

        return tokens
    }

    private fun findCommentStart(line: String): Int {
        var inString: Char? = null
        for (index in line.indices) {
            val ch = line[index]
            val quote = inString
            when {
                quote != null -> {
                    if (ch == quote && (index == 0 || line[index - 1] != '\\')) {
                        inString = null
                    }
                }
                ch == '"' || ch == '\'' -> inString = ch
                ch == '#' -> return index
            }
        }
        return line.length
    }

    private fun lineRange(rawLine: String, lineStart: Int, contentEnd: Int): TextRange {
        val first = rawLine.indexOfFirst { !it.isWhitespace() }.let { if (it < 0) 0 else it }
        return TextRange(lineStart + first, contentEnd)
    }

    private fun error(holder: AnnotationHolder, range: TextRange, message: String) {
        holder.newAnnotation(HighlightSeverity.ERROR, message)
            .range(range)
            .create()
    }

    private data class Token(
        val text: String,
        val range: TextRange,
    )

    private enum class Block {
        NONE,
        MODEL,
        ENUM,
    }

    private companion object {
        val declarationPattern = Regex("""(?:model|enum)\s+[A-Za-z_][A-Za-z0-9_]*\s*\{\s*""")
        val identifierPattern = Regex("""[A-Za-z_][A-Za-z0-9_]*""")
        val typeNamePattern = Regex("""[A-Za-z_][A-Za-z0-9_]*\??""")
        val computedPattern = Regex("""@computed\s+[A-Za-z_][A-Za-z0-9_]*\s*=\s*".*"\s*""")
        val modelAttributePattern = Regex("""@@[A-Za-z_][A-Za-z0-9_]*(?:\(.*\))?\s*""")
        val builtinTypes = setOf(
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
        val modifiers = setOf(
            "PrimaryKey",
            "NotNull",
            "Nullable",
            "Unique",
            "Index",
            "ForeignKey",
        )
        val fieldAttributes = setOf(
            "default",
            "audit",
        )
    }
}
