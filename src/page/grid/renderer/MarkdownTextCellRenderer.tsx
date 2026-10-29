import React from "react";
import Markdown from 'react-markdown';

export type MarkdownTextCellRendererProps = {
    value: string | null 
};

export function MarkdownTextCellRenderer(props: MarkdownTextCellRendererProps): React.JSX.Element {
    return <Markdown>{props.value || ''}</Markdown>;
}