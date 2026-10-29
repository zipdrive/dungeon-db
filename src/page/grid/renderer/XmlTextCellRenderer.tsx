import React from "react";
import { ColorTokens } from "code-colors-react";

export type XmlTextCellRendererProps = {
    value: string | null 
};

export function XmlTextCellRenderer(props: XmlTextCellRendererProps): React.JSX.Element {
    const beautifulXml: string = React.useMemo(() => {
        // TODO beautify XML
        return props.value || '';
    }, [props.value]);

    return <pre>
        <ColorTokens code={beautifulXml} lang="xml" />
    </pre>;
}