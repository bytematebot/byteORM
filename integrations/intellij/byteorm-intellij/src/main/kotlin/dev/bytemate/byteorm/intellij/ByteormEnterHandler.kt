package dev.bytemate.byteorm.intellij

import com.intellij.codeInsight.editorActions.enter.EnterHandlerDelegate
import com.intellij.codeInsight.editorActions.enter.EnterHandlerDelegateAdapter
import com.intellij.openapi.actionSystem.DataContext
import com.intellij.openapi.editor.Editor
import com.intellij.openapi.editor.actionSystem.EditorActionHandler
import com.intellij.openapi.project.DumbAware
import com.intellij.openapi.util.Ref
import com.intellij.psi.PsiFile

class ByteormEnterHandler : EnterHandlerDelegateAdapter(), DumbAware {
    override fun preprocessEnter(
        file: PsiFile,
        editor: Editor,
        caretOffset: Ref<Int>,
        caretAdvance: Ref<Int>,
        dataContext: DataContext,
        originalHandler: EditorActionHandler?,
    ): EnterHandlerDelegate.Result {
        if (!file.isByteormFile()) {
            return EnterHandlerDelegate.Result.Continue
        }

        val document = editor.document
        val text = document.charsSequence
        val offset = caretOffset.get().coerceIn(0, document.textLength)
        val lineNumber = document.getLineNumber(offset)
        val lineStart = document.getLineStartOffset(lineNumber)
        val lineEnd = document.getLineEndOffset(lineNumber)
        val previous = previousNonWhitespaceOffset(text, offset - 1, lineStart)

        if (previous == null || text[previous] != '{') {
            return EnterHandlerDelegate.Result.Continue
        }

        val next = nextNonWhitespaceOffset(text, offset, lineEnd)
        if (next != null && text[next] != '}') {
            return EnterHandlerDelegate.Result.Continue
        }

        val currentIndent = text.subSequence(lineStart, firstNonWhitespaceOffset(text, lineStart, lineEnd)).toString()
        val innerIndent = "$currentIndent    "

        if (next != null) {
            document.insertString(offset, "\n$innerIndent\n$currentIndent")
        } else {
            document.insertString(offset, "\n$innerIndent\n$currentIndent}")
        }

        editor.caretModel.moveToOffset(offset + 1 + innerIndent.length)
        return EnterHandlerDelegate.Result.Stop
    }

    private fun PsiFile.isByteormFile(): Boolean {
        return language.`is`(ByteormLanguage) || fileType == ByteormFileType
    }

    private fun previousNonWhitespaceOffset(text: CharSequence, start: Int, stopAt: Int): Int? {
        var index = start
        while (index >= stopAt) {
            if (!text[index].isWhitespace()) {
                return index
            }
            index--
        }
        return null
    }

    private fun nextNonWhitespaceOffset(text: CharSequence, start: Int, stopAt: Int): Int? {
        var index = start
        while (index < stopAt) {
            if (!text[index].isWhitespace()) {
                return index
            }
            index++
        }
        return null
    }

    private fun firstNonWhitespaceOffset(text: CharSequence, start: Int, stopAt: Int): Int {
        var index = start
        while (index < stopAt && text[index].isWhitespace()) {
            index++
        }
        return index
    }
}
