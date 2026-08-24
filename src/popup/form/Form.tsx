import { Checkbox, Input, Select, Typography } from "@material-tailwind/react";

export interface FormRootProps extends React.PropsWithChildren {
    title?: string
}

/**
 * A form of fields.
 */
export function FormRoot(props: FormRootProps): React.JSX.Element {
    if (props.title) {
        return (<div>
            <Typography variant="h4">{props.title}</Typography>
            <div>
                {props.children}
            </div>
        </div>)
    } else {
        return (<div>
            {props.children}
        </div>);
    }
}


export type FormTextEntryProps = {
    label: string,
    tooltip?: string,
    value: string,
    onSetValue: (newValue: string) => void,
};

/**
 * A text entry component on a form. 
 */
export function FormTextField(props: FormTextEntryProps): React.JSX.Element {
    return (<>
        <Typography variant="h6">{props.label}</Typography>
        <Input placeholder={props.label} value={props.value} onChange={(e) => { props.onSetValue(e.target.value); }} />
    </>);
}



export type FormSelectEntryProps<S extends string> = {
    label: string,
    tooltip?: string,
    value: S | undefined,
    possibleValues: { value: S, label: string, disabled?: boolean }[],
    onSetValue: (newValue: S) => void,
};

/**
 * A dropdown entry component on a form.
 */
export function FormSelectField<S extends string>(props: FormSelectEntryProps<S>): React.JSX.Element {
    return (<>
        <Typography variant="h6">{props.label}</Typography>
        <Select onValueChange={(s) => { props.onSetValue(s as S); }}>
            <Select.Trigger placeholder={props.label}></Select.Trigger>
            <Select.List>
                {props.possibleValues.map(({ value: optionValue, label: optionLabel, disabled: optionDisabled }) => 
                    (<Select.Option 
                        value={optionValue as string}
                        disabled={optionDisabled ?? false}
                    >
                        {optionLabel}
                    </Select.Option>)
                )}
            </Select.List>
        </Select>
    </>);
}



export type FormCheckboxEntryProps = {
    label: string,
    tooltip?: string,
    value: boolean,
    onSetValue: (newValue: boolean) => void,
};

/**
 * A checkbox entry component on a form.
 */
export function FormCheckboxField(props: FormCheckboxEntryProps): React.JSX.Element {
    return (<>
        <Typography variant="h6">{props.label}</Typography>
        <Checkbox checked={props.value} onChange={(e) => { props.onSetValue(e.target.checked); }} />
    </>);
}



export const Form = Object.assign(FormRoot, {
    TextField: FormTextField,
    SelectField: FormSelectField,
    CheckboxField: FormCheckboxField,
});
export default Form;