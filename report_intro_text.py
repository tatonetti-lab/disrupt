
import docx
import docx.document


def add_intro_text(doc : docx.document.Document ):

    p_header = doc.add_paragraph()
    title = p_header.add_run("DISRUPT :  PATIENT NOTIFICATION OF TRIAL MATCHING RESULTS")
    title.bold = True

    p = doc.add_paragraph()

    title = p.add_run("How we made this report")
    title.bold = True
    title.add_break()
    p.add_run("We looked at the information from your doctor about your cancer. This included details about the changes in your tumor, how your immune system is dealing with the tumor, and the stage of your tumor. We used this information to make a list of research studies at Columbia and other places that might be helpful for you and your treatment.")

    p2 = doc.add_paragraph()
    title = p2.add_run("How to use this report")
    title.bold = True
    title.add_break()
    p2.add_run("This report was made to include all the trials you might be able to join. There might be many that don't really apply to you even if we list them. We use our system to organize trials based on which ones are most likely to be right for you. We put these matches into different groups:")

    doc.add_paragraph("Excellent Match: You have a change in one or more genes (called a mutation) in your tumor and there's a trial for people with the same mutation.",
                       style='List Bullet')
    doc.add_paragraph("Good Match: A new kind of treatment is being tested for your type of tumor, but it might not be tested for the specific change in your tumor genes (mutation).",
                       style='List Bullet')
    doc.add_paragraph("Possible Match: We're not sure if this trial is right for your tumor, but we want you and your doctor to know about it just in case.",
                       style='List Bullet')
    doc.add_paragraph("Other Match: A possible match that is hard for us to classify",
                       style='List Bullet')

    doc.add_paragraph("Even if a study seems like it could be a good fit for you, there might be other reasons why it's not the right choice. For example, it might be a good match for people with lung cancer and your specific genetic mutation, but only for those who have already had surgery, and your doctor might not think you're a candidate for surgery. The best way to use this report is to talk about anything you're curious about with your doctor, who will also receive a copy of this report.")
    
    p3 = doc.add_paragraph()
    title = p3.add_run("What if I have questions about this report")
    title.bold = True
    title.add_break()
    p3.add_run("The best person to ask about this is your doctor. We do our best to provide accurate information, but there might be mistakes. Your doctor is the best person to confirm this. Also, information can change over time, including available trials. We do our best to keep this information up to date, but your doctor may know about trials not listed here.")

    p4 = doc.add_paragraph()
    title = p4.add_run("How to join a trial")
    title.bold = True
    title.add_break()
    p4.add_run("Even if we find a good trial for you, you may need to meet certain requirements to be part of the trial and receive the treatment. Each trial is different, so it's important to talk to your doctor about this. While we've tried to make the information easy to understand, it's always best to ask your doctor if you have any questions. Your doctor can provide you with the most accurate information and keep you updated on any new developments or trials that become available over time.").add_break()

    
    return doc