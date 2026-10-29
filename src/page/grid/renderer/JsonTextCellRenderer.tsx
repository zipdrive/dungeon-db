import React from "react";
import { ColorTokens } from "code-colors-react";

export type JsonTextCellRendererProps = {
    value: string | null 
};

export function JsonTextCellRenderer(props: JsonTextCellRendererProps): React.JSX.Element {
    const beautifulJson = React.useMemo(() => {
        try {
            return JSON.stringify(JSON.parse(props.value || ''), null, 2);
        } catch (_) {
            return props.value || '';
        }
    }, [props.value]);

    return <pre>
        <ColorTokens code={beautifulJson} lang="json" />
    </pre>;
}