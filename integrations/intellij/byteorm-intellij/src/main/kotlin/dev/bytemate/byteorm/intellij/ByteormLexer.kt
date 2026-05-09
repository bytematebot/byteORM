package dev.bytemate.byteorm.intellij

import com.intellij.lexer.LexerBase
import com.intellij.psi.TokenType
import com.intellij.psi.tree.IElementType

class ByteormLexer(
    private val symbols: ByteormSymbols? = null,
) : LexerBase() {
    private enum class BlockKind {
        MODEL,
        ENUM,
    }

    private var buffer: CharSequence = ""
    private var bufferEnd = 0
    private var tokenStart = 0
    private var tokenEnd = 0
    private var tokenType: IElementType? = null
    private var state = STATE_NORMAL
    private var parenDepth = 0
    private var pendingBlock: BlockKind? = null
    private var atLineStart = true
    private var lineWordIndex = 0

    override fun start(buffer: CharSequence, startOffset: Int, endOffset: Int, initialState: Int) {
        this.buffer = buffer
        this.bufferEnd = endOffset
        this.tokenStart = startOffset
        this.tokenEnd = startOffset
        this.tokenType = null
        this.state = decodeState(initialState)
        this.parenDepth = decodeParenDepth(initialState)
        this.pendingBlock = null
        this.atLineStart = startOffset == 0 || (startOffset > 0 && buffer[startOffset - 1] == '\n')
        this.lineWordIndex = 0
        locateToken()
    }

    override fun getState(): Int = encodeState(state, parenDepth)

    override fun getTokenType(): IElementType? = tokenType

    override fun getTokenStart(): Int = tokenStart

    override fun getTokenEnd(): Int = tokenEnd

    override fun getBufferSequence(): CharSequence = buffer

    override fun getBufferEnd(): Int = bufferEnd

    override fun advance() {
        tokenStart = tokenEnd
        locateToken()
    }

    private fun locateToken() {
        if (tokenStart >= bufferEnd) {
            tokenType = null
            tokenEnd = tokenStart
            return
        }

        when (val current = buffer[tokenStart]) {
            ' ', '\t', '\r', '\n' -> scanWhitespace()
            '#' -> scanComment()
            '"', '\'' -> scanString(current)
            in '0'..'9' -> scanNumber()
            '{' -> emitBrace(open = true)
            '}' -> emitBrace(open = false)
            '(' -> emitSimple(ByteormTokenTypes.L_PAREN)
            ')' -> emitSimple(ByteormTokenTypes.R_PAREN)
            ',' -> emitSimple(ByteormTokenTypes.COMMA)
            '.' -> emitSimple(ByteormTokenTypes.DOT)
            ':' -> emitSimple(ByteormTokenTypes.COLON)
            '=' -> emitSimple(ByteormTokenTypes.EQUALS)
            '@' -> emitSimple(ByteormTokenTypes.AT)
            '?' -> emitSimple(ByteormTokenTypes.QMARK)
            else -> if (current.isLetter() || current == '_') {
                scanWord()
            } else {
                tokenType = TokenType.BAD_CHARACTER
                tokenEnd = tokenStart + 1
                atLineStart = false
            }
        }
    }

    private fun emitSimple(type: IElementType) {
        tokenType = type
        tokenEnd = tokenStart + 1
        when (type) {
            ByteormTokenTypes.L_PAREN -> parenDepth++
            ByteormTokenTypes.R_PAREN -> parenDepth = (parenDepth - 1).coerceAtLeast(0)
        }
        atLineStart = false
    }

    private fun emitBrace(open: Boolean) {
        tokenType = if (open) ByteormTokenTypes.L_BRACE else ByteormTokenTypes.R_BRACE
        tokenEnd = tokenStart + 1
        if (open && state == STATE_AFTER_DECL_NAME) {
            state = when (pendingBlock) {
                BlockKind.MODEL -> STATE_MODEL_BODY
                BlockKind.ENUM -> STATE_ENUM_BODY
                null -> STATE_NORMAL
            }
            pendingBlock = null
            lineWordIndex = 0
        } else if (!open) {
            state = STATE_NORMAL
            pendingBlock = null
            lineWordIndex = 0
        }
        atLineStart = false
    }

    private fun scanWhitespace() {
        tokenType = ByteormTokenTypes.WHITE_SPACE
        var index = tokenStart
        var sawNewline = false
        while (index < bufferEnd && buffer[index].isWhitespace()) {
            if (buffer[index] == '\n') {
                sawNewline = true
                lineWordIndex = 0
            }
            index++
        }
        tokenEnd = index
        if (sawNewline) {
            atLineStart = true
        }
    }

    private fun scanComment() {
        tokenType = ByteormTokenTypes.COMMENT
        var index = tokenStart + 1
        while (index < bufferEnd && buffer[index] != '\n') {
            index++
        }
        tokenEnd = index
        atLineStart = false
    }

    private fun scanString(quote: Char) {
        tokenType = ByteormTokenTypes.STRING
        var index = tokenStart + 1
        var escaped = false
        while (index < bufferEnd) {
            val ch = buffer[index]
            if (escaped) {
                escaped = false
            } else if (ch == '\\') {
                escaped = true
            } else if (ch == quote) {
                index++
                break
            }
            index++
        }
        tokenEnd = index
        atLineStart = false
    }

    private fun scanNumber() {
        tokenType = ByteormTokenTypes.LITERAL
        var index = tokenStart
        while (index < bufferEnd) {
            val ch = buffer[index]
            if (ch.isDigit() || ch == '.' || ch == '_') {
                index++
            } else {
                break
            }
        }
        tokenEnd = index
        atLineStart = false
    }

    private fun scanWord() {
        var index = tokenStart
        while (index < bufferEnd) {
            val ch = buffer[index]
            if (ch.isLetterOrDigit() || ch == '_') {
                index++
            } else {
                break
            }
        }

        val text = buffer.subSequence(tokenStart, index).toString()
        tokenType = when {
            text == "model" || text == "enum" -> ByteormTokenTypes.KEYWORD
            state == STATE_EXPECT_DECL_NAME -> ByteormTokenTypes.DECLARATION_NAME
            state == STATE_ENUM_BODY -> ByteormTokenTypes.IDENTIFIER
            state == STATE_MODEL_BODY && lineWordIndex == 0 -> ByteormTokenTypes.IDENTIFIER
            state == STATE_MODEL_BODY && lineWordIndex == 1 -> {
                when {
                    text in byteormTypes -> ByteormTokenTypes.TYPE
                    text in knownEntityTypeNames() -> ByteormTokenTypes.ENTITY_TYPE
                    else -> ByteormTokenTypes.IDENTIFIER
                }
            }
            text == "default" || text == "computed" || text == "audit" -> ByteormTokenTypes.ATTRIBUTE
            text in literalWords -> ByteormTokenTypes.LITERAL
            text in modifierWords -> ByteormTokenTypes.MODIFIER
            text in byteormTypes -> ByteormTokenTypes.TYPE
            text in knownEntityTypeNames() -> ByteormTokenTypes.ENTITY_TYPE
            else -> ByteormTokenTypes.IDENTIFIER
        }

        when (text) {
            "model" -> {
                state = STATE_EXPECT_DECL_NAME
                pendingBlock = BlockKind.MODEL
            }
            "enum" -> {
                state = STATE_EXPECT_DECL_NAME
                pendingBlock = BlockKind.ENUM
            }
            else -> {
                if (tokenType == ByteormTokenTypes.DECLARATION_NAME) {
                    state = STATE_AFTER_DECL_NAME
                }
            }
        }

        if (state == STATE_MODEL_BODY || state == STATE_ENUM_BODY) {
            lineWordIndex++
        }

        tokenEnd = index
        atLineStart = false
    }

    private companion object {
        const val STATE_NORMAL = 0
        const val STATE_EXPECT_DECL_NAME = 1
        const val STATE_AFTER_DECL_NAME = 2
        const val STATE_MODEL_BODY = 3
        const val STATE_ENUM_BODY = 4
        const val STATE_MASK = 0xFF
        const val PAREN_SHIFT = 8

        val byteormTypes = setOf(
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

        val literalWords = setOf(
            "now",
            "true",
            "false",
        )

        val modifierWords = setOf(
            "ForeignKey",
            "Not",
            "Null",
            "PrimaryKey",
            "NotNull",
            "Nullable",
            "Unique",
            "Index",
            "onDelete",
            "cascade",
            "restrict",
            "set",
            "null",
            "no",
            "action",
        )

        fun encodeState(state: Int, parenDepth: Int): Int {
            return (parenDepth shl PAREN_SHIFT) or (state and STATE_MASK)
        }

        fun decodeState(encoded: Int): Int = encoded and STATE_MASK

        fun decodeParenDepth(encoded: Int): Int = encoded ushr PAREN_SHIFT
    }

    private fun knownEntityTypeNames(): Set<String> {
        return buildSet {
            symbols?.models?.let { addAll(it) }
            symbols?.enums?.let { addAll(it) }
        }
    }
}
