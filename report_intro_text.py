
import docx
import docx.document


def add_intro_text(doc : docx.document.Document ):

    p_header = doc.add_paragraph()
    title = p_header.add_run("ABOUT THIS REPORT")
    title.bold = True

    p = doc.add_paragraph()

    title = p.add_run("How we made this report")
    title.bold = True
    title.add_break()
    p.add_run("We used information from your doctor's notes describing your cancer. With this information, which includes the mutations in your tumor, the immune status of your tumor, and the stage of your tumor, we generated a list of clinical trials at Columbia and other participating sites that may be relevant to you and your treatment.")

    p2 = doc.add_paragraph()
    title = p2.add_run("How to use this report")
    title.bold = True
    title.add_break()
    p2.add_run("This report is designed to be inclusive - to include all possible trials for which you may be eligible. This means there may be a lot that also don't apply well even if we still list them. We use our algorithm to sort clinical trials based on which are most likely to be relevant to you. We sort these matches into the following categories:")

    doc.add_paragraph("Excellent Match: you have a mutation in your tumor and a trial is identified for patients with your mutation", style='List Bullet')
    doc.add_paragraph("Good Match: An immunotherapy or other novel drug is being tried in your tumor type, but it may not be tested directly for your specific mutation", style='List Bullet')
    doc.add_paragraph("Possible Match: We are not sure if this trial matches your tumor, but we want you and your physician to know about it in case it does", style='List Bullet')
    doc.add_paragraph("Other Match: A possible match that is hard for us to classify", style='List Bullet')

    doc.add_paragraph("Even if a study is a possible match, there may be other reasons why it is not appropriate for you. For example, it may be a good match for patients with lung cancer and your mutation - but apply only to patients after surgery and your doctor may not think you are eligible for surgery. Ultimately, the best use of this report is to discuss anything you are interested in with your treating physician, who also gets a copy of this report.")
    
    p3 = doc.add_paragraph()
    title = p3.add_run("What if I have questions about this report")
    title.bold = True
    title.add_break()
    p3.add_run("The person who can best answer questions about this is your doctor.  We try to be as accurate as possible, but there may sometimes be mistakes in our matching, and your doctor is best placed to let you know this. In addition, information can change over time, as well as available trials. We do our best to keep this up to date, but your doctor may also know of trials not included here.")

    p4 = doc.add_paragraph()
    title = p4.add_run("How to join a trial")
    title.bold = True
    title.add_break()
    p4.add_run("Even if we find a good trial for which you are a match, the trial may have additional criteria you need to pass to enroll in the trial and get the treatment. Every trial is a little different. Please discuss this with your doctor as well.").add_break()

    
    return doc